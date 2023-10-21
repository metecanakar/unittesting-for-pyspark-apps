"""Implementation of basic data enrichment"""
from pyspark.sql import DataFrame


def join_sales_with_weather(sales: DataFrame, weather: DataFrame):
    """
    Creates an inner join between the sales DataFrame and the weather DataFrame.
    Args:
        sales: Sales DataFrame
        weather: Weather DataFrame

    Returns:
        Enriched DataFrame
    """
    if sales.isEmpty():
        raise ValueError("Sales DataFrame should not be empty.")
    if weather.isEmpty():
        raise ValueError("Weather DataFrame should not be empty.")

    joined_df = sales.join(weather, on="date", how="inner")

    return joined_df
