from pydantic import BaseModel

class swapiChar(BaseModel):
    uid: int
    name: str
    url: str


class swapiDetails (BaseModel):
    name: str
    height: str
    mass: str
    hair_color: str
    skin_color: str
    eye_color: str
    birth_year: str
    gender: str
    homeworld: str
    


class swapiHomeworld(BaseModel):
    homeworld_name: str
    rotation_period: str
    orbital_period: str
    diameter: str
    climate: str
    gravity: str
    terrain: str
    surface_water: str
    population: str


