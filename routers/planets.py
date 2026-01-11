from fastapi import APIRouter, HTTPException
from typing import List


from schemas import *
from endpoints import get_all_planets, get_all_planets_by_id
from logger import log

planets_router = APIRouter(prefix="/planets", tags=["planets"])

@planets_router.get(path="/", response_model=List[swapiHomeworld])
async def get_planets():
    try:
        return get_all_planets.fetch_planets_from_db()
    except Exception as e:
        log.info("Error to get planets from the endpoint")
        return e
    

@planets_router.get(path="/{planet_id}")
async def get_planet(planet_id: str):
    try:
        plantet = get_all_planets_by_id.fetch_planet_from_db_by_id(planet_id)
        return plantet
    except Exception as e:
        log.info("Error to get planets by id from the endpoint")
        raise HTTPException(status_code=500, detail=str(e))