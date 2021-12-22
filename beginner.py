from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, collect_list, struct, udf
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

def initializeSpark():
    return SparkSession.builder\
        .master("local")\
        .config("spark.sql.autoBroadcastJoinThreshold", -1)\
        .config("spark.executor.memory", "500mb")\
        .appName("morisson")\
        .getOrCreate()

def concat_columns(df, is_last_node=False):
    if is_last_node:
        df = df.withColumn(\
            'children',  array().cast(ArrayType(StringType())))
    df = df.na.fill("[]", ["children"])
    return df.withColumn(\
        'concat', (struct(\
            col('label'), col('ID'),\
            col('link'), col('children'))))

def count_not_null(*args):
    i = 0
    for c in args:
        if c is not None:
            i += 1
    return i

def add_depth_column(df):
    my_udf = udf(lambda *x: count_not_null(*x), IntegerType())
    df_columns = [col(c) for c in df.columns if 'ID' in c]
    return df.withColumn('depth', my_udf(*df_columns))

def get_first_node(df, name, id, url, depth):
    return df.select(name, id, url)\
        .filter(df['depth'] == depth)\
        .withColumnRenamed(name, 'label')\
        .withColumnRenamed(id, 'ID')\
        .withColumnRenamed(url, 'link')

def get_subsequent_node(df, parent, name, id, url, depth):
    return df.select(parent, name, id, url)\
        .filter(df['depth'] == depth)\
        .withColumnRenamed(parent, 'pid')\
        .withColumnRenamed(name, 'label')\
        .withColumnRenamed(id, 'ID')\
        .withColumnRenamed(url, 'link')\

def aggregate_child_node(df):
    df = df.groupBy(df.pid).agg(collect_list(df.concat))
    return df.withColumnRenamed('collect_list(concat)', 'children')\
        .withColumnRenamed('pid', 'tempID')

def get_column_names(df, depth):
    pattern = str(depth) + str(' ')
    columns = [c for c in df.columns if pattern in c]
    name = ''
    url = ''
    id = ''
    for c in columns:
        if 'Name' in c:
            name = c
        if 'URL' in c:
            url = c
        if 'ID' in c:
            id = c
    return name, id, url

def get_max_depth(df):
    return df.agg({'depth': 'max'}).collect()[0][0]

def get_depth_node_dict(df, max_depth):
    depth_node_dict = {}
    for i in range(max_depth, 0, -1):
        if i == 1:
            columns = get_column_names(df, i)
            depth_node_dict[i] = get_first_node(\
                df, columns[0], columns[1], columns[2], i)
        else:
            columns = get_column_names(df, i)
            parent_id = 'Level ' + str(i-1) + str(' - ID')
            depth_node_dict[i] = get_subsequent_node(\
                df, parent_id, columns[0], columns[1], columns[2], i)
    return depth_node_dict

def combine_nodes(depth_node_dict, max_depth):
    agg = {}
    if max_depth == 1:
        formatted = concat_columns(depth_node_dict[1], True)
        agg[1] = formatted
        return agg

    for i in range(max_depth, 0, -1):
        if max_depth == i:
            formatted = concat_columns(depth_node_dict[i], True)
            agg[i] = aggregate_child_node(formatted)
        elif i == 1:
            agg[i] = depth_node_dict[i].join(\
                agg[i+1], depth_node_dict[i].ID == agg[i+1].tempID, 'left')
        else:
            merged = depth_node_dict[i].join(\
                agg[i+1], depth_node_dict[i].ID == agg[i+1].tempID, 'left')
            formatted = concat_columns(merged)
            agg[i] = aggregate_child_node(formatted)
    return agg

def create_result(agg, spark):
    if agg:
        result = agg[1].select('label', 'ID', 'link', 'children')
    else:
        schema = StructType([\
            StructField('label', StringType(), True),\
            StructField('ID', StringType(), True),\
            StructField('link', StringType(), True),\
            StructField('children', ArrayType(StructType()), True)])
        result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return result

def handle(path, spark):
    df = spark.read.csv(path, header="true")
    df = add_depth_column(df)

    max_depth = get_max_depth(df)
    depth_node_dict = get_depth_node_dict(df, max_depth)
    agg = combine_nodes(depth_node_dict, max_depth)
    return create_result(agg, spark)

def write_output(result, path = "/home/dindo/client/data.json"):
    result.write.json(path=path, ignoreNullFields=False, mode='overwrite')

# path = "/home/dindo/t2/c-gflow/test_csvs/original_data.csv"
# spark = initializeSpark()
# res = handle(path, spark)
# write_output(res)