"""
Transforms and Loads data into Azure Databricks
"""

import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv
import csv


def load_csv_to_db(csv_file_path, conn, table_name, create_table_sql):
    cursor = None
    try:
        cursor = conn.cursor()
        with open(csv_file_path, mode="r", newline="", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile)
            # Read the header row to get column names
            header = next(csv_reader)
            # Execute the provided CREATE TABLE SQL statement if any
            cursor.execute(create_table_sql)
            # Truncate the destination table
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            # Prepare the insert SQL statement
            sql_insert = f"INSERT INTO {table_name} VALUES "
            rows = []
            for row in csv_reader:
                processed_row = ["Null" if value == "" else value for value in row]
                rows.append(processed_row)
            # Generate placeholders dynamically based on the header
            placeholder_length = ", ".join(["?"] * len(header))
            # Prepare the full SQL statement with all values
            values_str = ", ".join(
                [f"({', '.join([repr(v) for v in row])})" for row in rows]
            )
            full_sql = sql_insert + values_str.replace("'Null'", "Null")
            print(full_sql)
            cursor.execute(full_sql)
        # Commit the transaction
        conn.commit()
        return True
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()


server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
http_path = os.getenv("HTTP")

create_script_arrest = """
CREATE TABLE IF NOT EXISTS ar805_arrest_DB (
                    country string,
                    violend int,
                    property int,
                    f_drugoff int,
                    f_sexoff int
                )
"""

create_script_population = """
CREATE TABLE IF NOT EXISTS ar805_population_DB (
                    state string,
                    country string,
                    total_population int
                )
"""


with sql.connect(
    server_hostname=server_h,
    http_path=http_path,
    access_token=access_token,
) as connection:

    load_csv_to_db(
        csv_file_path="data/arrest_data.csv",
        conn=connection,
        table_name="ar805_arrest_DB",
        create_table_sql=create_script_arrest,
    )

    load_csv_to_db(
        csv_file_path="data/population_data.csv",
        conn=connection,
        table_name="ar805_population_DB",
        create_table_sql=create_script_population,
    )
