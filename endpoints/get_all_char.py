import psycopg2
import os
from logger import log




def fetch_char_from_db():
    try:
        con = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", 5432))
        )
        cur = con.cursor()

        sql_char_selection = """
            SELECT
                c.uid, c.name, c.height, c.mass, c.hair_color, c.skin_color, c.eye_color,
                c.birth_year, c.gender, c.homeworld,
                p.homeworld_name, p.rotation_period, p.orbital_period, p.diameter,
                p.climate, p.gravity, p.terrain, p.surface_water, p.population
            FROM swapi_characters c
            LEFT JOIN swapi_planets p
                ON p.homeworld = c.homeworld;
        """

        cur.execute(sql_char_selection)
        records = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        cur.close()
        con.close()

        flat_rows = [dict(zip(columns, row)) for row in records]

        # Build nested output: character dict + nested homeworld dict
        nested_rows = []
        for r in flat_rows:
            homeworld_id = r.get("homeworld")

            homeworld_obj = None
            if r.get("homeworld_name") is not None:
                homeworld_obj = {
                    "homeworld": homeworld_id,
                    "homeworld_name": r.get("homeworld_name"),
                    "rotation_period": r.get("rotation_period"),
                    "orbital_period": r.get("orbital_period"),
                    "diameter": r.get("diameter"),
                    "climate": r.get("climate"),
                    "gravity": r.get("gravity"),
                    "terrain": r.get("terrain"),
                    "surface_water": r.get("surface_water"),
                    "population": r.get("population"),
                }


            r.pop("homeworld_name", None)
            r.pop("rotation_period", None)
            r.pop("orbital_period", None)
            r.pop("diameter", None)
            r.pop("climate", None)
            r.pop("gravity", None)
            r.pop("terrain", None)
            r.pop("surface_water", None)
            r.pop("population", None)

            r["homeworld"] = homeworld_obj

            nested_rows.append(r)

        return nested_rows

    except Exception as e:
        return e
