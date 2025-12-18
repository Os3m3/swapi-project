from fastapi import FastAPI
import uvicorn
import requests
import os

app = FastAPI()


SWAPI_TECH_URL = "https://swapi.tech/api/people"
SWAPI_HOMEWORLD_URL = "https://swapi.info/api/people"

file_path = "./data/swapi_char.csv"
file_path_details = "./data/swapi_details.csv"

@app.get("/characters")
def get_all_char():
    try:
        res = requests.get(SWAPI_TECH_URL)
        data = res.json()
        results = data["results"]

        next_url = data["next"]
        while next_url and len(results) <= 30:
            res = requests.get(next_url)
            data = res.json()
            results.extend(data["results"])
            next_url = data["next"]

        results = results[:29]

        if not os.path.exists(file_path):
            with open(file_path, "a") as csv_file:
                csv_file.write("uid,name,url\n")
                for person in results:
                    uid = person["uid"]
                    name = person["name"]
                    url = person["url"]
                    csv_file.write(f"{uid},{name},{url}\n")

        
        return results
    except Exception as e:
        return {"error": str(e)}
    


@app.get("/homeworld")
def get_all_homeworld():
    try:
        res = requests.get(SWAPI_HOMEWORLD_URL)
        data = res.json()
        result = data[:29]

        if not os.path.exists(file_path_details):
            with open(file_path_details, "a") as csv_file:
                csv_file.write("name,height,mass,hair_color,skin_color,eye_color,birth_year,gender,homeworld,films\n")
                for details in result:
                    name = details["name"]
                    height = details["height"]
                    mass = details["mass"]
                    hair_color = details["hair_color"]
                    skin_color = details["skin_color"]
                    eye_color = details["eye_color"]
                    birth_year = details["birth_year"]
                    gender = details["gender"]
                    homeworld = details["homeworld"]
                    films = details["films"]
                    csv_file.write(f"{name},{height},{mass},{hair_color},{skin_color},{eye_color},{birth_year},{gender},{homeworld},{films}\n")

    
    except Exception:
        print("Error on fetching data from secound database")




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)