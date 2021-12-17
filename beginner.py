from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, struct, udf
from pyspark.sql.types import ArrayType, IntegerType, StructType

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

spark = initializeSpark()

input_path = "/home/dindo/client/data.csv"
df = spark.read.csv(input_path, header="true")
df = add_count_column(df)

first_parent = get_first_node( \
    df, 'Level 1 - Name', \
    'Level 1 - ID', 'Level 1 - URL', 1)

second_parent = get_subsequent_node( \
    df, 'Level 1 - ID', 'Level 2 - Name', \
    'Level 2 - ID', 'Level 2 URL', 2)

third_parent = get_subsequent_node( \
    df, 'Level 2 - ID', 'Level 3 - Name', \
    'Level 3 - ID', 'Level 3 URL', 3)

third_df_raw = concat_columns(third_parent, True)
third_df_agg = aggregate_child_node(third_df_raw)

second_df_merged = second_parent.join( \
    third_df_agg, second_parent.ID == third_df_agg.tempID, 'left')

second_df_formatted = concat_columns(second_df_merged)
second_df_agg = aggregate_child_node(second_df_formatted)

first_df_merged = first_parent.join( \
    second_df_agg, first_parent.ID == second_df_agg.tempID, 'left')

res = first_df_merged.select('label', 'ID', 'link', 'children')

output_path = "/home/dindo/client/data.json"
res.write.json(output_path, 'overwrite')