from fastapi import FastAPI, HTTPException
import uvicorn
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor


from endpoints import char_endpoint, details_endpoint, homeworld_endpoint, get_all_char, get_all_char_by_id, get_all_planets, get_all_planets_by_id
from data_trans import merge_csv
from data_cleaning import data_cleaing
from schemas import *
from load_csv import *
from logger import log

executor = ThreadPoolExecutor(max_workers=1)

def etl_logic():
    fetch_all_char()
    fetch_all_details()
    fetch_all_homeworld()
    merg_all_csv()
    fetched_data_cleaning()
    data_load()


async def lifespan(app: FastAPI):

    async def watchdog_loop():
        loop = asyncio.get_running_loop()
        while True:
            try:
                await loop.run_in_executor(executor, etl_logic)
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                # clean shutdown
                break
            except Exception as e:
                print(f"Error: {e}")

    task = asyncio.create_task(watchdog_loop())

    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass



app = FastAPI(lifespan=lifespan)



SWAPI_TECH_URL = "https://www.swapi.tech/api/people"
SWAPI_DETAILS_URL = "https://www.swapi.info/api/people"
SWAPI_HOMEWORLD_URL = "https://www.swapi.info/api/planets"

file_path = "./data/swapi_char.csv"
file_path_details = "./data/swapi_details.csv"
file_path_homeworld= "./data/swapi_homeworld.csv"

@app.get("/characters_from_api", response_model=list[swapiChar])
def fetch_all_char() -> dict:
    try:
        return char_endpoint.get_all_char()
    except Exception as e:
        log.info("Error to get charecter from the endpoint")
        return e

    

@app.get("/details")
def fetch_all_details()-> dict:
    try:
        return details_endpoint.get_all_details()
    except Exception as e:
        log.info("Error to get charecter details from the endpoint")
        return e


@app.get("/homeworld")
def fetch_all_homeworld():
    return homeworld_endpoint.get_homeworld()


def merg_all_csv():
    try:
        return merge_csv()
    except Exception as e:
        log.info("Error to merge all CSV's together")
        return e
    


def fetched_data_cleaning():
    try:
        return data_cleaing()
    except Exception as e:
        log.info("Error on data cleaning process")
        return e
    

@app.post("/load-data")
def load_data():
    return data_load()



# Characteres

@app.get("/characters")
def get_characters():
    try:
        return get_all_char.fetch_char_from_db()
    except Exception as e:
        log.info("Error to get charecters from the endpoint")
        return e



@app.get("/character/{uid}")
def get_character(uid: str):
    result = get_all_char_by_id.fetch_char_from_db_by_id(uid)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Character {uid} not found")
    return result
        



# Planets ------------------------------
    
@app.get("/planets")
def get_planets():
    try:
        return get_all_planets.fetch_planets_from_db()
    except Exception as e:
        log.info("Error to get planets from the endpoint")
        return e



@app.get("/planets/{planet_id}")
def get_characters_by_id(planet_id: str):
    try:
        plantet = get_all_planets_by_id.fetch_planet_from_db_by_id(planet_id)
        return plantet
    except Exception as e:
        log.info("Error to get planets by id from the endpoint")
        raise HTTPException(status_code=500, detail=str(e))




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5555, reload=True)