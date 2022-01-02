from pyspark.sql import SparkSession

class Initializer:
    '''Function of class is to initialize spark session'''
    def initialize(self):
        return SparkSession.builder\
            .master("local")\
            .config("spark.sql.autoBroadcastJoinThreshold", -1)\
            .config("spark.executor.memory", "500mb")\
            .appName("morisson")\
            .getOrCreate()