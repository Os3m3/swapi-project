import psycopg2
import os
from logger import log


# def fetch_char_from_db_by_id (uid: str):
#     try:
#         con = psycopg2.connect(
#             dbname = os.getenv("POSTGRES_DB"),
#             user = os.getenv("POSTGRES_USER"),
#             password = os.getenv("POSTGRES_PASSWORD"),
#             host=os.getenv("POSTGRES_HOST"),
#             port=int(os.getenv("POSTGRES_PORT", 5432))     
#         )

#         cur = con.cursor()

#         sql_char_selection = "SELECT uid, name, height, mass, hair_color, skin_color, eye_color, birth_year, gender FROM swapi_characters WHERE uid = %s;"
#         cur.execute(sql_char_selection, (uid,))
#         record = cur.fetchone()
#         columns = [desc[0] for desc in cur.description]

#         cur.close()
#         con.close()

#         log.info(f"There is character with the {uid} id in the DB")
#         return dict(zip(columns, record))
    
#     except Exception as e:
#         log.info(f"There is no character with the {uid} id from DB")
#         return e

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
            SELECT uid, name, height, mass, hair_color, skin_color, eye_color, birth_year, gender
            FROM swapi_characters
            WHERE uid = %s;
        """
        cur.execute(sql, (uid,))
        record = cur.fetchone()

        if record is None:
            return None

        columns = [d[0] for d in cur.description]
        return dict(zip(columns, record))

    finally:
        if cur:
            cur.close()
        if con:
            con.close()
