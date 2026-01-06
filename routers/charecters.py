from fastapi import APIRouter, HTTPException
from typing import List


from endpoints import get_all_char, get_all_char_by_id
from logger import log
from schemas import *

router = APIRouter(prefix="/characters", tags=["characters"])


@router.get(path="/", response_model=List[swapiChar])
async def get_characters():
    try:
        return get_all_char.fetch_char_from_db()
    except Exception as e:
        log.info("Error to get charecters from the endpoint")
        return e
    


@router.get("/{uid}", response_model=swapiChar)
async def get_character(uid: str):
    result = get_all_char_by_id.fetch_char_from_db_by_id(uid)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Character {uid} not found")
    return result
