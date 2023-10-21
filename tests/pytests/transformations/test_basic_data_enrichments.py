"""Basic data enrichment Pytest example"""
import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytest


from spark_example_proj.transformations.basic_data_enrichments import join_sales_with_weather


@pytest.fixture(scope="module")
def spark_session():
    """
    Define a fixture to create a Spark session at the module level.

    If you want to group multiple tests in a class just create a TestClass
    (make sure that your class starts with the prefix "Test")
    without the need of subclassing anything. In this case, scope="class" can also be used in the fixture
    but for now there is no need for a test class.

    Once the fixture is created, provide it to other tests within the module.
    Finally, stop it once done during teardown of the last test in the module.
    So that they don’t mess with any other test modules.
    """
    # Create a Spark session
    spark = SparkSession.builder \
            .appName("pytest_spark_example") \
            .master("local[2]") \
            .getOrCreate()

    yield spark  # Provide the Spark session to test functions

    # TearDown (close the Spark session)
    spark.stop()


@pytest.fixture(scope="module")
def sales_schema():
    """Fixture to create sales schema"""
    sales_schema = StructType([
        StructField("sales_id", StringType(), True),
        StructField("gross_sales", DoubleType(), True),
        StructField("date", DateType(), True)
    ])

    return sales_schema


@pytest.fixture(scope="module")
def weather_schema():
    """Fixture to create weather schema"""
    weather_schema = StructType([
        StructField("date", DateType(), True),
        StructField("max_temp", IntegerType(), True),
        StructField("min_temp", IntegerType(), True),
        StructField("weather", StringType(), True)
    ])

    return weather_schema


@pytest.fixture(scope="module")
def expected_schema():
    """Fixture to create the expected schema"""
    expected_schema = StructType([
        StructField("sales_id", StringType(), True),
        StructField("gross_sales", DoubleType(), True),
        StructField("date", DateType(), True),
        StructField("max_temp", IntegerType(), True),
        StructField("min_temp", IntegerType(), True),
        StructField("weather", StringType(), True)
    ])
    return expected_schema


def test_join_sales_with_weather_when_all_days_are_matching(spark_session,
                                                            sales_schema,
                                                            weather_schema,
                                                            expected_schema):
    """
    Test the method join_sales_with_weather when all the days are matching.
    """
    # ------ PREP -----
    # Create the sales table
    sales_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16)),
                  (2, 11.0, datetime.date(year=2023, month=9, day=17))
                  ]
    sales_df = spark_session.createDataFrame(data=sales_rows, schema=sales_schema)

    # Create the weather table
    weather_rows = [(datetime.date(year=2023, month=9, day=16), 33, 24, "sunny"),
                    (datetime.date(year=2023, month=9, day=17), 31, 20, "sunny")]
    weather_df = spark_session.createDataFrame(data=weather_rows, schema=weather_schema)

    # expected data
    expected_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16), 33, 24, "sunny"),
                     (2, 11.0, datetime.date(year=2023, month=9, day=17), 31, 20, "sunny")]

    expected_df = spark_session.createDataFrame(data=expected_rows, schema=expected_schema)

    # ------ EXECUTE ------
    result_df = join_sales_with_weather(sales_df, weather_df)
    result_df = result_df.select("sales_id", "gross_sales", "date", "max_temp", "min_temp", "weather")

    # ------ EVAL-------
    assert_df_equality(result_df, expected_df)


def test_join_sales_with_weather_when_days_are_not_matching(spark_session,
                                                            sales_schema,
                                                            weather_schema,
                                                            expected_schema):
    """
    Test the method join_sales_with_weather when only 1 day is matching.
    The other 2 row from sales and weather dataframes are not matching.
    Then do not include them in the expected output.
    """
    # ------ PREP -----
    # Create the sales table
    sales_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16)), # matching
                  (2, 11.0, datetime.date(year=2023, month=9, day=18)) # not matching
                  ]

    sales_df = spark_session.createDataFrame(data=sales_rows, schema=sales_schema)

    # Create the weather table
    weather_rows = [(datetime.date(year=2023, month=9, day=16), 33, 24, "sunny"), # matching
                    (datetime.date(year=2023, month=9, day=17), 31, 20, "sunny") # not matching
                    ]
    weather_df = spark_session.createDataFrame(data=weather_rows, schema=weather_schema)

    # expected data
    expected_rows = [(1, 10.5, datetime.date(year=2023, month=9, day=16), 33, 24, "sunny")]
    expected_df = spark_session.createDataFrame(data=expected_rows, schema=expected_schema)

    # ------ EXECUTE ------
    result_df = join_sales_with_weather(sales_df, weather_df)
    result_df = result_df.select("sales_id", "gross_sales", "date", "max_temp", "min_temp", "weather")

    # ------ EVAL-------
    assert_df_equality(result_df, expected_df)
