from pyspark.sql.functions import array, col, collect_list, struct, when

class Decorator:
    '''Responsible for modifying the dataframe such as adding
        additional columns or combining rows together'''
    def calc_max_depth(self, df):
        columns = [c for c in df.columns if 'id' in c.lower()]
        return df.withColumn("levels", \
            sum(when(col(column).isNotNull(), 1) \
            .otherwise(0) for column in columns)) \
            .agg({"levels": "max"}).collect()[0][0]

    def calc_max_depth_test(self, dct):
        cols = dct.fieldnames
        depth = 0
        for i in cols:
            if 'id' in i:
                depth += 1

        return depth

    def aggregate_child_node(self, df):
        return df.groupBy(df.pid)\
            .agg(collect_list(df.concat).alias('children'))\
            .withColumnRenamed('pid', 'tempID')
    
    def concat_columns(self, df):
        return df.withColumn('concat', (struct( \
            col('label'), col('ID'),\
            col('link'), col('children'))))

    def remove_duplicates(self, df):
        return df.distinct()
    
    def fill_null_children(self, df):
        return df.withColumn( \
            "children", \
            when(df.children.isNull(), array()) \
            .otherwise(df.children))

    def append_empty_children(self, df):
        return df.withColumn("children", array())