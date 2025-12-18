from fastapi import FastAPI
import uvicorn
import requests
import csv
import pandas as pd
import os
import re

app = FastAPI()


SWAPI_TECH_URL = "https://swapi.tech/api/people"
SWAPI_DETAILS_URL = "https://swapi.info/api/people"
SWAPI_HOMEWORLD_URL = "https://swapi.info/api/planets"

file_path = "./data/swapi_char.csv"
file_path_details = "./data/swapi_details.csv"
file_path_homeworld= "./data/swapi_homeworld.csv"

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
    


@app.get("/details")
def get_all_details():
    try:
        res = requests.get(SWAPI_DETAILS_URL)
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




@app.get("/homeworld")
def get_homeworld():
    try:
        df = pd.read_csv(file_path_details)
        homeworld_url = df["homeworld"]
        patt = re.compile(r"^https?://.+/planets/\d+/?$")
        
        file_exists = os.path.exists(file_path_homeworld)

        with open(file_path_homeworld, "a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)

            if not file_exists:
                writer.writerow(["homeworld_name","rotation_period","orbital_period","diameter","climate","gravity","terrain","surface_water","population"])

            for url in homeworld_url:

                if not isinstance(url, str) or not patt.match(url):
                    writer.writerow(["None","None", "None", "None", "None","None", "None", "None", "None"])
                    continue

                # only valid URLs reach here
                res = requests.get(url)
                homeworld = res.json()

                writer.writerow([
                    homeworld.get("name"),
                    homeworld.get("rotation_period"),
                    homeworld.get("orbital_period"),
                    homeworld.get("diameter"),
                    homeworld.get("climate"),
                    homeworld.get("gravity"),
                    homeworld.get("terrain"),
                    homeworld.get("surface_water"),
                    homeworld.get("population"),
                ])

    except Exception as e:
        return {"error": str(e)}
    

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)