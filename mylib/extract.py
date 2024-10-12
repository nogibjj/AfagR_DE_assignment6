import requests
import os
import pandas as pd

# "https://raw.githubusercontent.com/nickeubank/practicaldatascience_class/refs/heads/master/Example_Data/ca/ca_arrests_2009.csv"


def extract(
    url="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv",
    file_path="data/usc_offers.csv",
    url2="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_commits.csv",
    file_path2="data/usc_commits.csv",
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
    df = pd.read_csv(file_path)
    df_2 = pd.read_csv(file_path2)

    df_subset = df.head(100)
    df_subset_2 = df_2.head(100)

    df_subset.to_csv(file_path, index=False)
    df_subset_2.to_csv(file_path2, index=False)
    return file_path, file_path2
