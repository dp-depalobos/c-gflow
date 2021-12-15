from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct

spark = SparkSession.builder.appName('Morrisons').getOrCreate()
df = spark.read.csv("dbfs:/FileStore/tables/morrisons/data.csv", header="true")

first_parent = df.select('Level 1 - Name', 'Level 1 - ID', 'Level 1 - URL') \
    .filter(df['Level 1 - ID'].isNotNull() & df['Level 2 - ID'].isNull()) \
    .withColumnRenamed('Level 1 - Name', 'label') \
    .withColumnRenamed('Level 1 - ID', 'ID') \
    .withColumnRenamed('Level 1 - URL', 'link')

second_parent = df.select('Level 1 - ID', 'Level 2 - Name', 'Level 2 - ID', 'Level 2 URL') \
    .filter(df['Level 2 - ID'].isNotNull() & df['Level 3 - ID'].isNull()) \
    .withColumnRenamed('Level 1 - ID', 'pid') \
    .withColumnRenamed('Level 2 - Name', 'label') \
    .withColumnRenamed('Level 2 - ID', 'ID') \
    .withColumnRenamed('Level 2 URL', 'link')

third_parent = df.select('Level 2 - ID', 'Level 3 - Name', 'Level 3 - ID', 'Level 3 URL') \
    .filter(df['Level 3 - ID'].isNotNull()) \
    .withColumnRenamed('Level 2 - ID', 'pid') \
    .withColumnRenamed('Level 3 - Name', 'label') \
    .withColumnRenamed('Level 3 - ID', 'ID') \
    .withColumnRenamed('Level 3 URL', 'link')

third_df_raw = third_parent.withColumn('concat', (struct(col('label'), col('ID'), col('link'))))
third_df_agg = third_df_raw.groupBy(third_df_raw.pid).agg(collect_list(third_df_raw.concat))

third_df_temp = third_df_agg.withColumnRenamed('collect_list(concat)', 'children').withColumnRenamed('pid', 'tempID')
second_df_raw = second_parent.withColumn('concat', (struct(col('label'), col('ID'), col('link'))))
second_df_merged = second_df_raw.join(third_df_temp, second_df_raw.ID == third_df_temp.tempID, 'left')

second_df_formatted = second_df_merged.withColumn('concat', (struct(col('label'), col('ID'), col('link'), col('children'))))
second_df_agg = second_df_formatted.groupBy(second_df_formatted.pid).agg(collect_list(second_df_formatted.concat))

second_df_temp = second_df_agg.withColumnRenamed('collect_list(concat)', 'children').withColumnRenamed('pid', 'tempID')
first_df_raw = first_parent.withColumn('concat', (struct(col('label'), col('ID'), col('link'))))
first_df_merged = first_df_raw.join(second_df_temp, first_df_raw.ID == second_df_temp.tempID, 'left')

first_df_formatted = first_df_merged.withColumn('concat', (struct(col('label'), col('ID'), col('link'), col('children'))))

res = first_df_formatted.select('label', 'ID', 'link', 'children')
res.write.json("dbfs:/FileStore/tables/morrisons/result/data.json")
