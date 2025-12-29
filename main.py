from fastapi import FastAPI, HTTPException
import uvicorn
import asyncio
from contextlib import asynccontextmanager
import typing


from endpoints import char_endpoint, details_endpoint, homeworld_endpoint, get_all_char, get_all_char_by_id
from data_trans import merge_csv
from data_cleaning import data_cleaing
from schemas import *
from database.load_csv import *




@asynccontextmanager
async def lifespan(app: FastAPI):
    async def watchdog_loop():
        while True:
            print("tick")
            try:
                fetch_all_char()
                fetch_all_details()
                fetch_all_homeworld()
                merg_all_csv()
                data_cleaning()
                data_load()
            except Exception as e:
                print("Watchdog error:", e)
            await asyncio.sleep(30)

    task = asyncio.create_task(watchdog_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
# app = FastAPI()



SWAPI_TECH_URL = "https://www.swapi.tech/api/people"
SWAPI_DETAILS_URL = "https://www.swapi.info/api/people"
SWAPI_HOMEWORLD_URL = "https://www.swapi.info/api/planets"

file_path = "./data/swapi_char.csv"
file_path_details = "./data/swapi_details.csv"
file_path_homeworld= "./data/swapi_homeworld.csv"

@app.get("/characters", response_model=list[swapiChar])
def fetch_all_char() -> dict:
    try:
        return char_endpoint.get_all_char()
    except Exception as e:
        return {"error": str(e)}

    

@app.get("/details", response_model=swapiDetails)
def fetch_all_details()-> dict:
    try:
        return details_endpoint.get_all_details()
    except Exception as e:
        return {"error": str(e)}



@app.get("/homeworld", response_model=swapiHomeworld)
def fetch_all_homeworld() -> dict:
    try:
        return homeworld_endpoint.get_homeworld()
    except Exception as e:
        return {"error": str(e)}
    


def merg_all_csv():
    try:
        return merge_csv()
    except Exception as e:
        return {"error": str(e)} 
    


def data_cleaning():
    try:
        return data_cleaing()
    except Exception as e:
        return {"error": str(e)}
    

for _ in data_load():
        pass


@app.get("/characters")
def get_characters():
    try:
        return get_all_char.fetch_char_from_db()
    except Exception:
        print("Error")




@app.get("/characters/{uid}")
def get_characters_by_id(uid: str):
    try:
        char = get_all_char_by_id.fetch_char_from_db_by_id(uid)
        return char
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


    

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)