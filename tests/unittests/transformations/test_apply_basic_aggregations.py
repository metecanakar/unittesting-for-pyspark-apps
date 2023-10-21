import unittest
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField
from chispa.dataframe_comparer import assert_df_equality

from spark_example_proj.transformations.apply_basic_aggregations import calculate_monthly_avg_sales


class TestDataEnrichment(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Initialize Spark session only once for the entire test class using 2 local threads
        cls.spark = SparkSession.builder.master("local[2]").appName("TestApp").getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        # Stop the Spark session after all test methods have been executed
        cls.spark.stop()

    def test_calculate_monthly_avg_sales(self):
        """
        Test the method calculate_monthly_avg_sales.
        """
        # ------ PREP -----
        input_rows = [(1, 10, datetime.date(2023, 9, 16), 33, 24, "sunny"), # 202309
                      (1, 20, datetime.date(2023, 9, 17), 31, 21, "sunny"),
                      (1, 20, datetime.date(2023, 10, 16), 22, 14, "sunny"), # 202310
                      (1, 40, datetime.date(2023, 10, 17), 20, 15, "cloudy"),
                      (1, 50, datetime.date(2023, 11, 16), 15, 5, "rainy") # 202311
                      ]
        input_cols = ["sales_id", "gross_sales", "date", "max_temp", "min_temp", "weather"]
        input_df = self.spark.createDataFrame(data=input_rows, schema=input_cols)

        # expected data
        expected_rows = [(202309, 15.0),
                         (202310, 30.0),
                         (202311, 50.0)
                         ]
        expected_schema = StructType([
            StructField("month", IntegerType(), True),
            StructField("avg_sales", DoubleType(), True)])
        expected_df = self.spark.createDataFrame(data=expected_rows, schema=expected_schema)

        # ------ EXECUTE ------
        result_df = calculate_monthly_avg_sales(input_df)

        # ------ EVAL-------
        assert_df_equality(result_df, expected_df)


