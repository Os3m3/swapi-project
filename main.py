from fastapi import FastAPI
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List

from endpoints import char_endpoint, details_endpoint, homeworld_endpoint
from data_trans import merge_csv
from data_cleaning import data_cleaing
from schemas import *
from load_csv import *
from logger import log

from routers.charecters import router as characters_router
from routers.planets import planets_router as planets_router

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

@app.get("/characters_from_api", response_model=List[swapiChar])
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


app.include_router(characters_router)
app.include_router(planets_router)

    
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)