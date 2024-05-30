"""
Snowflake data staging script
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

# Stage Location
STAGE_LOCATION = "@amazon_sales_dwh.source.data_stg"


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


def traverse_directory(
    directory: str, file_extension: str
) -> tuple[list[str], list[str], list[str]]:
    """Traverse the directory to get filename, partition and local file path.

    Args:
        directory (str): The path of the directory to traverse.
        file_extension (str): The file extension to filter files.

    Returns:
        Tuple[List[str], List[str], List[str]]: A tuple containing three lists:
            - A list of filenames with the specified extension.
            - A list of partition directories relative to the base directory.
            - A list of local file paths corresponding to the filenames.
    """
    local_file_path = []
    file_name = []  # List to store file paths
    partition_dir = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)

    return file_name, partition_dir, local_file_path


def upload_files(
    session: Session,
    file_names: list[str],
    partition_dirs: list[str],
    local_file_paths: list[str],
    stage_location: str,
) -> None:
    """Upload files to the Snowflake stage.

    Args:
        session (SnowflakeConnection): An active Snowflake session.
        file_names (List[str]): A list of filenames to be uploaded.
        partition_dirs (List[str]): A list of partition directories corresponding to the files.
        local_file_paths (List[str]): A list of local file paths for the files to be uploaded.
        stage_location (str): The target stage location in Snowflake.

    Returns:
        None
    """
    for index, file_element in enumerate(file_names):
        try:
            put_result = session.file.put(
                local_file_paths[index],
                f"{stage_location}/{partition_dirs[index]}",
                auto_compress=False,
                overwrite=True,
                parallel=10,
            )
            logging.info("Uploaded %s => %s", file_element, put_result[0].status)
        except Exception as e:
            logging.error("Error uploading %s: %s", file_element, e)


def main():
    """Entrypoint"""

    session = get_snowpark_session()

    try:
        logging.info("Session created successfully")

        # Specify the directory path to traverse
        directory_path = "./amazon-sales-data/"

        # Traverse directory for CSV, Parquet, and JSON files
        csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(
            directory_path, ".csv"
        )
        parquet_file_name, parquet_partition_dir, parquet_local_file_path = (
            traverse_directory(directory_path, ".parquet")
        )
        json_file_name, json_partition_dir, json_local_file_path = traverse_directory(
            directory_path, ".json"
        )

        # Upload files to Snowflake stage
        upload_files(
            session,
            csv_file_name,
            csv_partition_dir,
            csv_local_file_path,
            STAGE_LOCATION,
        )
        upload_files(
            session,
            parquet_file_name,
            parquet_partition_dir,
            parquet_local_file_path,
            STAGE_LOCATION,
        )
        upload_files(
            session,
            json_file_name,
            json_partition_dir,
            json_local_file_path,
            STAGE_LOCATION,
        )
    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        if session:
            session.close()
            logging.info("Session closed")


if __name__ == "__main__":
    main()
