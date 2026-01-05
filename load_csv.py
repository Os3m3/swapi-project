import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

csv_loc = "./data/cleaned_dataframe.csv"

def data_load():
    con = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"), 
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

    cur = con.cursor()

    try:

        cur.execute("""
        CREATE TABLE IF NOT EXISTS swapi_characters (
            uid TEXT PRIMARY KEY,
            name TEXT,
            url TEXT,
            height TEXT,
            mass TEXT,
            hair_color TEXT,
            skin_color TEXT,
            eye_color TEXT,
            birth_year TEXT,
            gender TEXT,
            homeworld TEXT,
            homeworld_name TEXT,
            rotation_period TEXT,
            orbital_period TEXT,
            diameter TEXT,
            climate TEXT,
            gravity TEXT,
            terrain TEXT,
            surface_water TEXT,
            population TEXT
        );
        """)

        cur.execute("TRUNCATE TABLE swapi_characters;")

        # 3) Load CSV
        with open(csv_loc, "r", encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY swapi_characters
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )

        con.commit()

    finally:
        cur.close()
        con.close()