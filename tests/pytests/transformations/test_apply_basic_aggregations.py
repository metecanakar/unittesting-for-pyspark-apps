import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField
from chispa.dataframe_comparer import assert_df_equality
import pytest

from spark_example_proj.transformations.apply_basic_aggregations import calculate_monthly_avg_sales


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


def test_calculate_monthly_avg_sales(spark_session):
    """
    Test the method calculate_monthly_avg_sales.
    """
    # ------ PREP -----
    input_rows = [(1, 10, datetime.date(2023, 9, 16), 33, 24, "sunny"),  # 202309
                  (1, 20, datetime.date(2023, 9, 17), 31, 21, "sunny"),
                  (1, 20, datetime.date(2023, 10, 16), 22, 14, "sunny"),  # 202310
                  (1, 40, datetime.date(2023, 10, 17), 20, 15, "cloudy"),
                  (1, 50, datetime.date(2023, 11, 16), 15, 5, "rainy")  # 202311
                  ]
    input_cols = ["sales_id", "gross_sales", "date", "max_temp", "min_temp", "weather"]
    input_df = spark_session.createDataFrame(data=input_rows, schema=input_cols)

    # expected data
    expected_rows = [(202309, 15.0),
                     (202310, 30.0),
                     (202311, 50.0)
                     ]
    expected_schema = StructType([
        StructField("month", IntegerType(), True),
        StructField("avg_sales", DoubleType(), True)])
    expected_df = spark_session.createDataFrame(data=expected_rows, schema=expected_schema)

    # ------ EXECUTE ------
    result_df = calculate_monthly_avg_sales(input_df)

    # ------ EVAL-------
    assert_df_equality(result_df, expected_df)




