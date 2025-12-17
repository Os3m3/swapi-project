from fastapi import FastAPI
import uvicorn
import requests
import os

app = FastAPI()


SWAPI_BASE_URL = "https://swapi.tech/api/people"

file_path = "./data/swapi_char.csv"

@app.get("/characters")
def get_all_char():
    try:
        res = requests.get(SWAPI_BASE_URL)
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
            with open(file_path, "w") as csv_file:
                csv_file.write("uid,name,url\n")

        
        return results
    except Exception as e:
        return {"error": str(e)}




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)