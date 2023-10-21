"""Basic data enrichment unittest example"""
import datetime
import unittest

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from spark_example_proj.transformations.basic_data_enrichments import join_sales_with_weather


class TestDataEnrichment(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Initialize Spark session only once for the entire test class using 2 local threads
        cls.spark = SparkSession.builder.master("local[2]").appName("TestApp").getOrCreate()

        # define the schemas on the class level which will be shared across all the tests within the class
        cls.sales_schema = StructType([
            StructField("sales_id", StringType(), True),
            StructField("gross_sales", DoubleType(), True),
            StructField("date", DateType(), True)
        ])

        cls.weather_schema = StructType([
            StructField("date", DateType(), True),
            StructField("max_temp", IntegerType(), True),
            StructField("min_temp", IntegerType(), True),
            StructField("weather", StringType(), True)
        ])

        cls.expected_schema = StructType([
            StructField("sales_id", StringType(), True),
            StructField("gross_sales", DoubleType(), True),
            StructField("date", DateType(), True),
            StructField("max_temp", IntegerType(), True),
            StructField("min_temp", IntegerType(), True),
            StructField("weather", StringType(), True)
        ])

    @classmethod
    def tearDownClass(cls) -> None:
        # Stop the Spark session after all test methods have been executed
        cls.spark.stop()

    def test_join_sales_with_weather_when_all_days_are_matching(self):
        """
        Test the method join_sales_with_weather when all the days are matching.
        """
        # ------ PREP -----
        # Create the sales table
        sales_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16)),
                      (2, 11.0, datetime.date(year=2023, month=9, day=17))
                      ]
        sales_df = self.spark.createDataFrame(data=sales_rows, schema=self.sales_schema)

        # Create the weather table
        weather_rows = [(datetime.date(year=2023, month=9, day=16), 33, 24, "sunny"),
                        (datetime.date(year=2023, month=9, day=17), 31, 20, "sunny")]
        weather_df = self.spark.createDataFrame(data=weather_rows, schema=self.weather_schema)

        # expected data
        expected_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16), 33, 24, "sunny"),
                         (2, 11.0, datetime.date(year=2023, month=9, day=17), 31, 20, "sunny")]

        expected_df = self.spark.createDataFrame(data=expected_rows, schema=self.expected_schema)

        # ------ EXECUTE ------
        result_df = join_sales_with_weather(sales_df, weather_df)
        result_df = result_df.select("sales_id", "gross_sales", "date", "max_temp", "min_temp", "weather")

        # ------ EVAL-------
        assert_df_equality(result_df, expected_df)

    def test_join_sales_with_weather_when_days_are_not_matching(self):
        """
        Test the method join_sales_with_weather when only 1 day is matching.
        The other 2 row from sales and weather dataframes are not matching.
        Then do not include them in the expected output.
        """
        # ------ PREP -----
        # Create the sales table
        sales_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16)),  # matching
                      (2, 11.0, datetime.date(year=2023, month=9, day=18))  # not matching
                      ]

        sales_df = self.spark.createDataFrame(data=sales_rows, schema=self.sales_schema)

        # Create the weather table
        weather_rows = [(datetime.date(year=2023, month=9, day=16), 33, 24, "sunny"),  # matching
                        (datetime.date(year=2023, month=9, day=17), 31, 20, "sunny")  # not matching
                        ]
        weather_df = self.spark.createDataFrame(data=weather_rows, schema=self.weather_schema)

        # expected data
        expected_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16), 33, 24, "sunny")]
        expected_df = self.spark.createDataFrame(data=expected_rows, schema=self.expected_schema)

        # ------ EXECUTE ------
        result_df = join_sales_with_weather(sales_df, weather_df)
        result_df = result_df.select("sales_id", "gross_sales", "date", "max_temp", "min_temp", "weather")

        # ------ EVAL-------
        assert_df_equality(result_df, expected_df)


