"""Query the database from a db connection to Azure Databricks"""

import os
from databricks import sql
from dotenv import load_dotenv


query = """
           select aa.state, sum(aa.total_population) state_population,
            sum(bb.violent) violence_arrest, round((violence_arrest/state_population)*100,1) as violence_percent
            from `ids706_data_engineering`.`default`.`ar805_population_db` aa
            left join ids706_data_engineering.default.ar805_arrest_db bb
            on aa.county = bb.county
            group by aa.state
            having violence_arrest is not null
            order by violence_arrest desc;
            """

# Define a global variable for the log file
LOG_FILE = "query_log.md"


def log_query(query, result="none"):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")
        file.write(f"```response from databricks\n{result}\n```\n\n")


def general_query(query):
    """runs a query a user inputs"""

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
        c.execute(query)
        result = c.fetchall()
    c.close()
    log_query(f"{query}", result)
    return "query successfull"


if __name__ == "__main__":
    general_query(query)
