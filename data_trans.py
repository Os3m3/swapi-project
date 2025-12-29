import pandas as pd


def merge_csv():
    swapi_char = pd.read_csv("data/swapi_char.csv")
    swapi_details = pd.read_csv("data/swapi_details.csv")
    swapi_homeworld = pd.read_csv("data/swapi_homeworld.csv")
    df = pd.concat([swapi_char,swapi_details, swapi_homeworld], axis=1)
    df = df.iloc[:, :3].join(df.iloc[:, 4:])
    df.to_csv("data\swapi_all_merged.csv", index=False)
    return df

