from pydantic import BaseModel

class swapiChar(BaseModel):
    name:str
    uid: str
    url: str