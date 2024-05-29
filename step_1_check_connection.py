"""
    Snowflake connection script
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


# snowpark session
def get_snowpark_session() -> Session:
    """Create Snowpark Session"""
    connection_parameters = {
        "ACCOUNT_ID": os.environ.get("ACCOUNT_ID"),
        "USER": os.environ.get("SF_USER"),
        "PASSWORD": os.environ.get("PASSWORD"),
        "ROLE": os.environ.get("ROLE"),
        "DATABASE": os.environ.get("DATABASE"),
        "SCHEMA": os.environ.get("SCHEMA"),
        "WAREHOUSE": os.environ.get("WAREHOUSE"),
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def main():
    """Entrypoint"""
    session = get_snowpark_session()

    context_df = session.sql(
        "select \
            current_role(),\
            current_database(),\
            current_schema(),\
            current_warehouse()"
    )
    context_df.show(2)

    customer_df = session.sql(
        "select \
            c_custkey, \
            c_name, \
            c_phone, \
            c_mktsegment \
         from \
            snowflake_sample_data.tpch_sf1.customer \
         limit 10"
    )
    customer_df.show(5)


if __name__ == "__main__":
    main()
