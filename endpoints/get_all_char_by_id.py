import psycopg2
from psycopg2 import DatabaseError
import os


def fetch_char_from_db_by_id (uid: str):
    con = psycopg2.connect(
        dbname = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=5432        
    )

    cur = con.cursor()

    sql_char_selection = "SELECT name, height, mass, hair_color, skin_color, eye_color, birth_year, gender FROM swapi_characters WHERE uid = %s;"
    cur.execute(sql_char_selection, (uid,))
    record = cur.fetchone()
    columns = [desc[0] for desc in cur.description]

    cur.close()
    con.close()

    return dict(zip(columns, record))