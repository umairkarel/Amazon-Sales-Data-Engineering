"""
Snowflake transfer data from source to curated layer
"""

import os
import sys
import logging
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit, rank
from snowflake.snowpark import Window
from dotenv import load_dotenv

load_dotenv()

# initiate logging at info level
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%I:%M:%S",
)


# snowpark session
def get_snowpark_session() -> Session:
    """Create Snowpark Session"""
    connection_parameters = {
        "ACCOUNT": os.environ.get("ACCOUNT_ID"),
        "USER": os.environ.get("SF_USER"),
        "PASSWORD": os.environ.get("PASSWORD"),
        "ROLE": os.environ.get("ROLE"),
        "DATABASE": os.environ.get("DATABASE"),
        "SCHEMA": os.environ.get("SCHEMA"),
        "WAREHOUSE": os.environ.get("WAREHOUSE"),
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def filter_dataset(df: DataFrame, column_name: str, filter_value: str) -> DataFrame:
    """
    Filter the DataFrame based on a specified column and value.

    This function filters the input DataFrame to include only the rows where the specified column's
    value matches the given value.

    Args:
        df (DataFrame): The input DataFrame to be filtered.
        column_name (str): The name of the column on which the filter is to be applied.
        filter_value (str): The value to filter the rows by.

    Returns:
        DataFrame: A new DataFrame containing only the rows that match the filter value.

    Example:
        filtered_df = filter_dataset(df, 'PAYMENT_STATUS', 'Paid')
    """
    return df.filter(col(column_name) == filter_value)


def process_sales_order(country_code: str):
    """
    Process sales orders for a given country and save the curated data.

    This function fetches sales orders from a specific country, filters the data to include only
    paid and delivered orders, adds country and region information, performs a join with a
    forex exchange rate table, de-duplicates the data, and saves the final curated data to a table.

    Args:
        country_code (str): The country code for which the sales orders need to be processed.
                            Supported country codes are 'US', 'IN', and 'FR'.
                            'US' for United States
                            'IN' for India
                            'FR' for France

    Raises:
        ValueError: If the provided country_code is not supported.
    """

    # Dictionary to map country codes to relevant details
    country_details = {
        "US": ("us_sales_order", "USD", "US", "AMER", "USD2USD"),
        "IN": ("in_sales_order", "INR", "IN", "APAC", "USD2INR"),
        "FR": ("fr_sales_order", "EUR", "FR", "EU", "USD2EU"),
    }

    # Fetch country-specific details
    sales_order_table, local_currency, country, region, exchange_rate_col = (
        country_details[country_code]
    )

    # Get the session object and get dataframe
    session = get_snowpark_session()
    sales_df = session.sql(f"SELECT * FROM {sales_order_table}")

    # Apply filter to select only paid and delivered sale orders
    paid_sales_df = filter_dataset(sales_df, "PAYMENT_STATUS", "Paid")
    shipped_sales_df = filter_dataset(paid_sales_df, "SHIPPING_STATUS", "Delivered")

    # Adding country and region to the data frame
    country_sales_df = shipped_sales_df.with_column(
        "Country", lit(country)
    ).with_column("Region", lit(region))

    # Join to add forex calculation
    forex_df = session.sql("SELECT * FROM amazon_sales_dwh.common.exchange_rate")
    sales_with_forext_df = country_sales_df.join(
        forex_df,
        country_sales_df["order_dt"] == forex_df["date"],
        join_type="outer",
    )

    # De-duplication
    unique_orders = (
        sales_with_forext_df.with_column(
            "order_rank",
            rank().over(
                Window.partitionBy(col("order_dt")).order_by(
                    col("_metadata_last_modified").desc()
                )
            ),
        )
        .filter(col("order_rank") == 1)
        .select(col("SALES_ORDER_KEY").alias("unique_sales_order_key"))
    )
    print(unique_orders)
    final_sales_df = unique_orders.join(
        sales_with_forext_df,
        unique_orders["unique_sales_order_key"]
        == sales_with_forext_df["SALES_ORDER_KEY"],
        join_type="inner",
    )

    # Select final columns
    final_sales_df = final_sales_df.select(
        col("SALES_ORDER_KEY"),
        col("ORDER_ID"),
        col("ORDER_DT"),
        col("CUSTOMER_NAME"),
        col("MOBILE_KEY"),
        col("Country"),
        col("Region"),
        col("ORDER_QUANTITY"),
        lit(local_currency).alias("LOCAL_CURRENCY"),
        col("UNIT_PRICE").alias("LOCAL_UNIT_PRICE"),
        col("PROMOTION_CODE").alias("PROMOTION_CODE"),
        col("FINAL_ORDER_AMOUNT").alias("LOCAL_TOTAL_ORDER_AMT"),
        col("TAX_AMOUNT").alias("local_tax_amt"),
        col(exchange_rate_col).alias("Exchange_Rate"),
        (col("FINAL_ORDER_AMOUNT") / col(exchange_rate_col)).alias(
            "US_TOTAL_ORDER_AMT"
        ),
        (col("TAX_AMOUNT") / col(exchange_rate_col)).alias("USD_TAX_AMT"),
        col("payment_status"),
        col("shipping_status"),
        col("payment_method"),
        col("payment_provider"),
        (
            col("phone").alias("contact_no")
            if country_code in ("US", "FR")
            else col("mobile").alias("contact_no")
        ),
        col("shipping_address"),
    )

    # Write final dataframe to the corresponding curated table
    final_sales_df.write.save_as_table(
        f"amazon_sales_dwh.curated.{country.lower()}_sales_order", mode="append"
    )


def main():
    """Entrypoint"""

    process_sales_order("IN")
    process_sales_order("US")
    process_sales_order("FR")


if __name__ == "__main__":
    main()
