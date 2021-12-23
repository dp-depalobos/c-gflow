import unittest
from pandas.testing import assert_frame_equal

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

import handler

class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = SparkSession\
            .builder\
            .appName("Test PySpark")\
            .master("local[*]")\
            .getOrCreate()
        cls.spark = spark
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def get_schema(self, depth):
        if depth == 0:
            return StringType()
        depth -= 1
        return StructType([
            StructField('label', StringType()),\
            StructField('ID', StringType()),\
            StructField('link', StringType()),\
            StructField('children', ArrayType(self.get_schema(depth)))])

class test_handler(TestBase):
    def test_should_create_empty_df_when_empty_rows(self):
        path = "/home/dindo/t2/c-gflow/test_csvs/empty_rows.csv"
        result = handler.handle(path, self.spark)
        self.assertTrue(result.rdd.isEmpty())

    def test_should_produce_correct_json_file_level_one(self):
        path = "/home/dindo/t2/c-gflow/test_csvs/level_one.csv"
        result = handler.handle(path, self.spark)
        schema = self.get_schema(1)
        check = self.spark.read.json(\
            "/home/dindo/t2/c-gflow/test_results_json/level_one.json", schema)
        result_pandas = result.toPandas()
        format_pandas = check.toPandas()
        assert_frame_equal(result_pandas, format_pandas)

    def test_should_produce_correct_json_file_level_two(self):
        path = "/home/dindo/t2/c-gflow/test_csvs/level_two.csv"
        result = handler.handle(path, self.spark)
        schema = self.get_schema(2)
        check = self.spark.read.json(\
            "/home/dindo/t2/c-gflow/test_results_json/level_two.json",\
            schema=schema)
        result_pandas = result.toPandas()
        format_pandas = check.toPandas()
        assert_frame_equal(result_pandas, format_pandas)

    def test_should_produce_correct_json_for_original_data(self):
        path = "/home/dindo/t2/c-gflow/test_csvs/original_data.csv"
        result = handler.handle(path, self.spark)

        schema = self.get_schema(3)
        check = self.spark.read.json(\
            "/home/dindo/t2/c-gflow/test_results_json/original_data.json",\
            schema=schema)
        result_pandas = result.toPandas()
        format_pandas = check.toPandas()
        assert_frame_equal(result_pandas, format_pandas)

    def test_should_produce_correct_json_for_level_four(self):
        path = "/home/dindo/t2/c-gflow/test_csvs/level_four.csv"
        result = handler.handle(path, self.spark)
        schema = self.get_schema(4)
        check = self.spark.read.json(\
            "/home/dindo/t2/c-gflow/test_results_json/level_four.json",\
            schema=schema)
        result_pandas = result.toPandas()
        format_pandas = check.toPandas()
        assert_frame_equal(result_pandas, format_pandas)
        
if __name__ == '__main__':
    unittest.main()