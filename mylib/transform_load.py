"""
Transforms and Loads data into Azure Databricks
"""

import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv


# load the csv file and insert into databricks
def load(dataset="data/usc_offers.csv", dataset2="data/usc_commits.csv"):
    """Transforms and Loads data into the local databricks database"""
    df = pd.read_csv(dataset, delimiter=",", skiprows=1)
    df2 = pd.read_csv(dataset2, delimiter=",", skiprows=1)
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP")
    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        # INSERT TAKES TOO LONG
        # c.execute("DROP TABLE IF EXISTS ServeTimesDB")
        c.execute("SHOW TABLES FROM default LIKE 'serve*'")
        result = c.fetchall()
        # takes too long so not dropping anymore
        # c.execute("DROP TABLE IF EXISTS EventTimesDB")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS cfb_players_offers_DB (
                    name string,
                    school string,
                    city string,
                    ranking_offers int
                    
                )
            """
            )
            # insert
            for _, row in df.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO cfb_players_offers_DB VALUES {convert}")
        c.execute("SHOW TABLES FROM default LIKE 'cfb_p*'")
        result = c.fetchall()
        # c.execute("DROP TABLE IF EXISTS EventTimesDB")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS cfb_players_commits_DB (
                    name string,
                    school string,
                    city string,
                    ranking_commits int
                )
                """
            )
            for _, row in df2.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO cfb_players_commits_DB VALUES {convert}")
        c.close()

    return "success"
