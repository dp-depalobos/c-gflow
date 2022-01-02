import os
import unittest
from pandas.testing import assert_frame_equal

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import col

from src import handler

LEVEL_ONE = 1
LEVEL_THREE = 3
LEVEL_SIX = 6
LEVEL_TEN = 10
CSV = ".csv"
FILES = "files"
JSON = ".json"
OUTPUT = "output"
ONE_ROW = "one_row"
ORIGINAL_DATA = "original_data"
ORIGINAL_DUPLICATE_RECORD = "original_duplicate_record"
ORIGINAL_ONE_LEVEL = "original_one_level_data"
UPTO_LEVEL_6 = "original_data_upto_level_6"
UPTO_LEVEL_10 = "original_data_upto_level_10"

class TestBase(unittest.TestCase):
    current = os.getcwd()

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

    def get_file_path(self, folder, filename, ext):
        return os.path.join(self.current, folder, filename + str(ext))

    def read_check(self, depth, path):
        schema = self.get_schema(depth)
        return self.spark.read.json(\
            path, schema)

    def compare_nested(self, left, right):
        left_format = self.convert_list_to_str(left).toPandas()
        right_format = self.convert_list_to_str(right).toPandas()
        assert_frame_equal(left_format, right_format)
    
    def compare_level_one(self, left, right):
        left_format = left.toPandas()
        right_format = right.toPandas()
        assert_frame_equal(left_format, right_format)

    def convert_list_to_str(self, df):
        """This method is necessary for comparing the children column since it is originally an array type"""
        return df.withColumn('children', col('children').cast('String'))

class test_handler(TestBase):
    def test_should_produce_correct_json_file_one_row(self):
        input_path = self.get_file_path(FILES, ONE_ROW, CSV)
        result = handler.handle(input_path, self.spark)
        check_path = self.get_file_path(OUTPUT, ONE_ROW, JSON)
        check = self.read_check(LEVEL_THREE, check_path)
        self.compare_level_one(result, check)

    def test_should_produce_correct_json_file_level_six(self):
        input_path = self.get_file_path(FILES, UPTO_LEVEL_6, CSV)
        result = handler.handle(input_path, self.spark)
        check_path = self.get_file_path(OUTPUT, UPTO_LEVEL_6, JSON)
        check = self.read_check(LEVEL_SIX, check_path)
        self.compare_nested(result, check)

    def test_should_produce_correct_json_for_level_10(self):
        input_path = self.get_file_path(FILES, UPTO_LEVEL_10, CSV)
        result = handler.handle(input_path, self.spark)
        check_path = self.get_file_path(OUTPUT, UPTO_LEVEL_10, JSON)
        check = self.read_check(LEVEL_TEN, check_path)
        self.compare_nested(result, check)

    def test_should_produce_correct_json_for_original_data(self):
        input_path = self.get_file_path(FILES, ORIGINAL_DATA, CSV)
        result = handler.handle(input_path, self.spark)
        check_path = self.get_file_path(OUTPUT, ORIGINAL_DATA, JSON)
        check = self.read_check(LEVEL_THREE, check_path)
        self.compare_nested(result, check)
        
    def test_should_produce_correct_json_for_original_data_with_duplicate(self):
        input_path = self.get_file_path(FILES, ORIGINAL_DUPLICATE_RECORD, CSV)
        result = handler.handle(input_path, self.spark)
        check_path = self.get_file_path(OUTPUT, ORIGINAL_DUPLICATE_RECORD, JSON)
        check = self.read_check(LEVEL_SIX, check_path)
        self.compare_nested(result, check)

    def test_should_produce_correct_json_for_one_level_duplicate(self):
        input_path = self.get_file_path(FILES, ORIGINAL_ONE_LEVEL, CSV)
        result = handler.handle(input_path, self.spark)
        check_path = self.get_file_path(OUTPUT, ORIGINAL_ONE_LEVEL, JSON)
        check = self.read_check(LEVEL_ONE, check_path)
        self.compare_level_one(result, check)

if __name__ == '__main__':
    unittest.main()