from pyspark.sql.functions import array, col, collect_list, struct, when
from pyspark.sql.types import ArrayType, StringType

class Decorator:
    '''Responsible for modifying the dataframe such as adding
        additional rows our combining columns together'''
    def add_depth_column(self, df):
        columns = [c for c in df.columns if 'id' in c.lower()]
        return df.withColumn("depth", \
            sum([when(col(column).isNotNull(), \
                1).otherwise(0) for column in columns]))

    def calc_max_depth(self, df):
        return df.agg({'depth': 'max'}).collect()[0][0]

    def aggregate_child_node(self, df):
        return df.groupBy(df.pid)\
            .agg(collect_list(df.concat).alias('children'))\
            .withColumnRenamed('pid', 'tempID')
    
    def concat_columns(self, df, is_last_node=False):
        if is_last_node:
            df = df.withColumn(\
                'children',  array().cast(ArrayType(StringType())))
        return df.withColumn(\
            'concat', (struct(\
                col('label'), col('ID'),\
                col('link'), col('children'))))
