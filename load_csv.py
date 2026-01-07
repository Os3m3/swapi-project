import psycopg2
import os
from dotenv import load_dotenv
import csv
from psycopg2.extras import execute_batch

load_dotenv()

csv_loc = "./data/cleaned_dataframe.csv"

def data_load():
    con = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

    cur = con.cursor()

    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS swapi_planets (
            homeworld TEXT PRIMARY KEY,
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

        cur.execute("""
        CREATE TABLE IF NOT EXISTS swapi_characters (
            uid INT PRIMARY KEY,
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
            CONSTRAINT fk_homeworld
                FOREIGN KEY (homeworld)
                REFERENCES swapi_planets(homeworld)
                ON DELETE SET NULL
        );
        """)

        cur.execute("TRUNCATE TABLE swapi_characters, swapi_planets;")
        
        planets = {}
        characters = []

        with open(csv_loc, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                homeworld = (row.get("homeworld") or "").strip() or None

                if homeworld and homeworld not in planets:
                    planets[homeworld] = (
                        homeworld,
                        (row.get("homeworld_name") or "").strip() or None,
                        (row.get("rotation_period") or "").strip() or None,
                        (row.get("orbital_period") or "").strip() or None,
                        (row.get("diameter") or "").strip() or None,
                        (row.get("climate") or "").strip() or None,
                        (row.get("gravity") or "").strip() or None,
                        (row.get("terrain") or "").strip() or None,
                        (row.get("surface_water") or "").strip() or None,
                        (row.get("population") or "").strip() or None,
                    )

                characters.append(
                    (
                        int(float(row["uid"])),  # handles "1.0"
                        (row.get("name") or "").strip() or None,
                        (row.get("url") or "").strip() or None,
                        (row.get("height") or "").strip() or None,
                        (row.get("mass") or "").strip() or None,
                        (row.get("hair_color") or "").strip() or None,
                        (row.get("skin_color") or "").strip() or None,
                        (row.get("eye_color") or "").strip() or None,
                        (row.get("birth_year") or "").strip() or None,
                        (row.get("gender") or "").strip() or None,
                        homeworld,
                    )
                )

        # 5) Insert planets first
        execute_batch(cur, """
            INSERT INTO swapi_planets (
                homeworld, homeworld_name, rotation_period, orbital_period, diameter,
                climate, gravity, terrain, surface_water, population
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (homeworld) DO NOTHING
        """, list(planets.values()), page_size=500)

        # 6) Insert characters second
        execute_batch(cur, """
            INSERT INTO swapi_characters (
                uid, name, url, height, mass, hair_color, skin_color,
                eye_color, birth_year, gender, homeworld
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (uid) DO NOTHING
        """, characters, page_size=500)

        con.commit()

    except Exception as e:
        con.rollback()
        raise RuntimeError(f"DB load failed: {e}") from e

    finally:
        cur.close()
        con.close()