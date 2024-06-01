"""
Snowflake load data from staging to source tables
"""

import os
import sys
import logging
from snowflake.snowpark import Session
from dotenv import load_dotenv

load_dotenv()

# initiate logging at info level
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%I:%M:%S",
)


COUNTRY_SQL_STATEMENTS = {
    "in": """
        COPY INTO amazon_sales_dwh.source.in_sales_order
        FROM (
            SELECT
                amazon_sales_dwh.source.in_sales_order_seq.NEXTVAL,
                $1::text AS order_id,
                $2::text AS customer_name,
                $3::text AS mobile_key,
                $4::number AS order_quantity,
                $5::number AS unit_price,
                $6::number AS order_value,
                $7::text AS promotion_code,
                $8::number(10,2) AS final_order_amount,
                $9::number(10,2) AS tax_amount,
                t.$10::date AS order_dt,
                t.$11::text AS payment_status,
                t.$12::text AS shipping_status,
                t.$13::text AS payment_method,
                t.$14::text AS payment_provider,
                t.$15::text AS mobile,
                t.$16::text AS shipping_address,
                metadata$filename AS stg_file_name,
                metadata$file_row_number AS stg_row_number,
                metadata$file_last_modified AS stg_last_modified
            FROM
                @amazon_sales_dwh.source.data_stg/sales/source=IN/format=csv/
                (file_format => 'amazon_sales_dwh.common.my_csv_format') t
        )
        ON_ERROR = CONTINUE
        """,
    "us": """
        COPY INTO amazon_sales_dwh.source.us_sales_order
        FROM (
            SELECT
                amazon_sales_dwh.source.us_sales_order_seq.NEXTVAL,
                $1:"Order ID"::text AS order_id,
                $1:"Customer Name"::text AS customer_name,
                $1:"Mobile Model"::text AS mobile_key,
                TO_NUMBER($1:"Quantity") AS quantity,
                TO_NUMBER($1:"Price per Unit") AS unit_price,
                TO_DECIMAL($1:"Total Price") AS total_price,
                $1:"Promotion Code"::text AS promotion_code,
                $1:"Order Amount"::number(10,2) AS order_amount,
                TO_DECIMAL($1:"Tax") AS tax,
                $1:"Order Date"::date AS order_dt,
                $1:"Payment Status"::text AS payment_status,
                $1:"Shipping Status"::text AS shipping_status,
                $1:"Payment Method"::text AS payment_method,
                $1:"Payment Provider"::text AS payment_provider,
                $1:"Phone"::text AS phone,
                $1:"Delivery Address"::text AS shipping_address,
                metadata$filename AS stg_file_name,
                metadata$file_row_number AS stg_row_number,
                metadata$file_last_modified AS stg_last_modified
            FROM
                @amazon_sales_dwh.source.data_stg/sales/source=US/format=parquet/
                (file_format => amazon_sales_dwh.common.my_parquet_format)
        )
        ON_ERROR = CONTINUE
        """,
    "fr": """
        COPY INTO amazon_sales_dwh.source.fr_sales_order
        FROM (
            SELECT
                amazon_sales_dwh.source.fr_sales_order_seq.NEXTVAL,
                $1:"Order ID"::text AS order_id,
                $1:"Customer Name"::text AS customer_name,
                $1:"Mobile Model"::text AS mobile_key,
                TO_NUMBER($1:"Quantity") AS quantity,
                TO_NUMBER($1:"Price per Unit") AS unit_price,
                TO_DECIMAL($1:"Total Price") AS total_price,
                $1:"Promotion Code"::text AS promotion_code,
                $1:"Order Amount"::number(10,2) AS order_amount,
                TO_DECIMAL($1:"Tax") AS tax,
                $1:"Order Date"::date AS order_dt,
                $1:"Payment Status"::text AS payment_status,
                $1:"Shipping Status"::text AS shipping_status,
                $1:"Payment Method"::text AS payment_method,
                $1:"Payment Provider"::text AS payment_provider,
                $1:"Phone"::text AS phone,
                $1:"Delivery Address"::text AS shipping_address,
                metadata$filename AS stg_file_name,
                metadata$file_row_number AS stg_row_number,
                metadata$file_last_modified AS stg_last_modified
            FROM
                @amazon_sales_dwh.source.data_stg/sales/source=FR/format=json/
                (file_format => amazon_sales_dwh.common.my_json_format)
        )
        ON_ERROR = CONTINUE
        """,
}


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


def ingest_sales(session: Session, country_code: str) -> None:
    """
    Ingests sales orders from a staged CSV file into the respective table in the data warehouse.

    Parameters:
        session (Session): The Snowflake session object used to execute the SQL query.
        country_code (str): The country code of data to ingest.

    Returns:
        None
    """
    session.sql(COUNTRY_SQL_STATEMENTS.get(country_code, "")).collect()


def main():
    """Entrypoint"""

    # get the session object and get dataframe
    session = get_snowpark_session()

    # ingest india sales data
    ingest_sales(session, "in")

    # ingest us sales data
    ingest_sales(session, "us")

    # ingest france sales data
    ingest_sales(session, "fr")


if __name__ == "__main__":
    main()
