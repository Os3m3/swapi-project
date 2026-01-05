import psycopg2
import os
from logger import log


def fetch_planets_from_db ():
    try:
        con = psycopg2.connect(
            dbname = os.getenv("POSTGRES_DB"),
            user = os.getenv("POSTGRES_USER"),
            password = os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", 5432))        
        )

        cur = con.cursor()

        sql_char_selection = "SELECT homeworld, homeworld_name, orbital_period, diameter, climate, gravity, terrain, surface_water, population FROM swapi_characters;"
        cur.execute(sql_char_selection)
        records = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        cur.close()
        con.close()

        result = [dict(zip(columns, row)) for row in records]
        log.info("Got all planets from DB")
        return result
    
    except Exception as e:
        log.info("Error on getting all planets from DB")
        return e