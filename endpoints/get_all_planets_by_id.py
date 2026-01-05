import psycopg2
import os
from logger import log


def fetch_planet_from_db_by_id (planet_id: str):
    try:
        target_url = f"https://swapi.info/api/planets/{planet_id}"
        con = psycopg2.connect(
            dbname = os.getenv("POSTGRES_DB"),
            user = os.getenv("POSTGRES_USER"),
            password = os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", 5432)) 
        )

        cur = con.cursor()

        sql_char_selection = "SELECT homeworld, homeworld_name, orbital_period, diameter, climate, gravity, terrain, surface_water, population FROM swapi_characters WHERE homeworld = %s;"
        cur.execute(sql_char_selection, (target_url,))
        record = cur.fetchone()
        columns = [desc[0] for desc in cur.description]

        cur.close()
        con.close()
        log.info(f"There is planet with the {planet_id} id in the DB")
        return dict(zip(columns, record))
    
    except Exception as e:
        log.info(f"Error on getting planet {planet_id} id from DB")
        return e