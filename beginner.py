from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, struct, udf
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

def initializeSpark():
    return SparkSession.builder \
        .master("local") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.executor.memory", "500mb") \
        .appName("morisson") \
        .getOrCreate()

def concat_columns(df, is_last_node=False):
    if is_last_node:
        df = df.withColumn( \
            'children', lit(None).cast(ArrayType(StructType())))
    return df.withColumn( \
        'concat', (struct( \
            col('label'), col('ID'), \
            col('link'), col('children'))))

def count_not_null(*args):
    i = 0
    for c in args:
        if c is not None:
            i += 1
    return i

def add_count_column(df):
    my_udf = udf(lambda *x: count_not_null(*x), IntegerType())
    df_columns = [col(c) for c in df.columns if 'ID' in c]
    return df.withColumn("count", my_udf(*df_columns))

def get_first_node(df, name, id, url, depth):
    return df.select(name, id, url) \
        .filter(df['count'] == depth) \
        .withColumnRenamed(name, 'label') \
        .withColumnRenamed(id, 'ID') \
        .withColumnRenamed(url, 'link')

def get_subsequent_node(df, parent, name, id, url, depth):
    return df.select(parent, name, id, url) \
        .filter(df['count'] == depth) \
        .withColumnRenamed(parent, 'pid') \
        .withColumnRenamed(name, 'label') \
        .withColumnRenamed(id, 'ID') \
        .withColumnRenamed(url, 'link') \

def aggregate_child_node(df):
    df = df.groupBy(df.pid).agg(collect_list(df.concat))
    return df.withColumnRenamed('collect_list(concat)', 'children') \
        .withColumnRenamed('pid', 'tempID')

def get_column_names(df, depth):
    columns = [c for c in df.columns if str(depth) in c]
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

def get_max_count(df):
    return df.agg({'count': 'max'}).collect()[0][0]

def get_depth_node_map(df, max_count):
    depth_node_map = {}
    for i in range(max_count, 0, -1):
        if i == 1:
            columns = get_column_names(df, i)
            depth_node_map[i] = get_first_node(\
                df, columns[0], columns[1], columns[2], i)
        else:
            columns = get_column_names(df, i)
            parent_id = 'Level ' + str(i-1) + str(' - ID')
            depth_node_map[i] = get_subsequent_node(\
                df, parent_id, columns[0], columns[1], columns[2], i)
    return depth_node_map

spark = initializeSpark()

input_path = "/home/dindo/client/data3.csv"
df = spark.read.csv(input_path, header="true")
df = add_count_column(df)

max_count = get_max_count(df)
depth_node_map = get_depth_node_map(df, max_count)

agg = {}
for i in range(max_count, 0, -1):
    if max_count == i:
        formatted = concat_columns(depth_node_map[i], True)
        agg[i] = aggregate_child_node(formatted)
    elif i == 1:
        agg[i] = depth_node_map[i].join( \
            agg[i+1], depth_node_map[i].ID == agg[i+1].tempID, 'left')
    else:
        merged = depth_node_map[i].join( \
            agg[i+1], depth_node_map[i].ID == agg[i+1].tempID, 'left')
        formatted = concat_columns(merged)
        agg[i] = aggregate_child_node(formatted)

schema = StructType([ \
    StructField('label', StringType(), True), \
    StructField('ID', StringType(), True), \
    StructField('link', StringType(), True), \
    StructField('children', ArrayType(StructType()), True)])

if agg:
    res = agg[1].select('label', 'ID', 'link', 'children')
else:
    res = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

output_path = "/home/dindo/client/data.json"
res.write.json(output_path, 'overwrite')