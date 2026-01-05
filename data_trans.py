import pandas as pd
from pathlib import Path


DATA_DIR = Path("data")
MERGED_CSV = DATA_DIR / "swapi_all_merged.csv"


def merge_csv():
    """
    Merge the three fetched CSVs into a single file in the ./data directory.
    Using pathlib keeps paths OS-agnostic (fixes Linux runs that previously wrote
    to 'data\\swapi_all_merged.csv' in the project root).
    """
    DATA_DIR.mkdir(exist_ok=True)

    swapi_char = pd.read_csv(DATA_DIR / "swapi_char.csv")
    swapi_details = pd.read_csv(DATA_DIR / "swapi_details.csv")
    swapi_homeworld = pd.read_csv(DATA_DIR / "swapi_homeworld.csv")

    df = pd.concat([swapi_char, swapi_details, swapi_homeworld], axis=1)
    df = df.iloc[:, :3].join(df.iloc[:, 4:])

    df.to_csv(MERGED_CSV, index=False)
    return df

