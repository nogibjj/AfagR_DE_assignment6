import requests
import os
import pandas as pd
import random


def extract(
    url="https://raw.githubusercontent.com/nickeubank/practicaldatascience_class/refs/heads/master/Example_Data/ca/ca_arrests_2009.csv",
    file_path="data/arrest_data.csv",
    url2="https://raw.githubusercontent.com/nickeubank/practicaldatascience_class/refs/heads/master/Example_Data/ca/nhgis_county_populations.csv",
    file_path2="data/population_data.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    with requests.get(url2) as r:
        with open(file_path2, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(file_path, index_col=0)
    df = df[["COUNTY", "VIOLENT", "PROPERTY", "F_DRUGOFF", "F_SEXOFF"]]
    random.seed(123)
    df_2 = pd.read_csv(file_path2, index_col=0)
    df_2 = df_2[df_2["YEAR"] == "2005-2009"][["STATE", "COUNTY", "total_population"]]
    df_subset_2 = df_2.sample(600)

    df.to_csv(file_path, index=False)
    df_subset_2.to_csv(file_path2, index=False)
    return file_path, file_path2
