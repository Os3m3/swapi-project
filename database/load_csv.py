import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

csv_loc = "./data/cleaned_dataframe.csv"

def data_load():
    con = psycopg2.connect(
        dbname = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=5432
    )

    cur = con.cursor()

    try:
        cur.execute("TRUNCATE TABLE swapi_characters;")
        with open(csv_loc, "r", encoding="utf-8") as f:
            cur.copy_expert(
                "COPY swapi_characters FROM STDIN WITH CSV HEADER",
                f
            )

        con.commit()
        yield cur
    finally:
        cur.close()
        con.close()