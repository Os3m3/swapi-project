import psycopg2
import os
from logger import log

def fetch_char_from_db_by_id(uid: str):
    con = None
    cur = None
    try:
        con = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", 5432))
        )
        cur = con.cursor()

        sql = """
            SELECT
                c.uid,
                c.name,
                c.height,
                c.mass,
                c.hair_color,
                c.skin_color,
                c.eye_color,
                c.birth_year,
                c.gender,
                c.homeworld,

                p.homeworld_name,
                p.rotation_period,
                p.orbital_period,
                p.diameter,
                p.climate,
                p.gravity,
                p.terrain,
                p.surface_water,
                p.population
            FROM swapi_characters c
            LEFT JOIN swapi_planets p
                ON p.homeworld = c.homeworld
            WHERE c.uid = %s
            LIMIT 1;
        """

        cur.execute(sql, (uid,))
        record = cur.fetchone()

        if record is None:
            return None

        columns = [d[0] for d in cur.description]
        row = dict(zip(columns, record))


        homeworld_obj = None
        if row.get("homeworld_name") is not None:
            homeworld_obj = {
                "homeworld": row.get("homeworld"),
                "homeworld_name": row.get("homeworld_name"),
                "rotation_period": row.get("rotation_period"),
                "orbital_period": row.get("orbital_period"),
                "diameter": row.get("diameter"),
                "climate": row.get("climate"),
                "gravity": row.get("gravity"),
                "terrain": row.get("terrain"),
                "surface_water": row.get("surface_water"),
                "population": row.get("population"),
            }

        row.pop("homeworld_name", None)
        row.pop("rotation_period", None)
        row.pop("orbital_period", None)
        row.pop("diameter", None)
        row.pop("climate", None)
        row.pop("gravity", None)
        row.pop("terrain", None)
        row.pop("surface_water", None)
        row.pop("population", None)

        row["homeworld"] = homeworld_obj

        return row

    finally:
        if cur:
            cur.close()
        if con:
            con.close()