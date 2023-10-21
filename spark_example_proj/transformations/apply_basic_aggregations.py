"""Implementation of applying basic aggregations"""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def calculate_monthly_avg_sales(joined_df: DataFrame) -> DataFrame:
    """
    Takes an input joined_df (enriched DataFrame), adds a new month column.
    and finally calculates the average sales for each month.
    Args:
        joined_df: Enriched sales DataFrame with the weather DataFrame
    Returns:
        Aggregated sales for each month.
        Schema:
            StructField("month", IntegerType(), True),
            StructField("avg_sales", DoubleType(), True)])

    """
    df_month = joined_df.withColumn("month", (F.date_format("date", "yyyyMM")).cast("int"))
    df_avg_sales = df_month.groupBy(["month"]).agg(F.avg("gross_sales").alias("avg_sales"))

    return df_avg_sales

