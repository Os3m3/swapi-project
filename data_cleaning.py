import pandas as pd
import numpy as np


def data_cleaing():
    df = pd.read_csv("./data/swapi_all_merged.csv")
    unknown_values = ["", "unknown", "Unknown", "null", "None"]
    df = df.replace(unknown_values, np.nan)
    df = df.dropna()
    df.to_csv("./data/cleaned_dataframe.csv", index=False)
    return df