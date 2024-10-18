from mylib.extract import extract
from databricks import sql
from dotenv import load_dotenv
import os
import subprocess


def test_extract():
    extracted_data = extract()
    assert extracted_data is not None


def test_load():
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP"),
        access_token=os.getenv("ACCESS_TOKEN"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM  ar805_arrest_DB")
            result = cursor.fetchall()
            cursor.close()
            connection.close()
    assert result is not None


def test_query():
    """tests general_query"""
    result = subprocess.run(
        [
            "python",
            "main.py",
            "general_query",
            "query",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


if __name__ == "__main__":
    test_extract()
    test_load()
    test_query()
