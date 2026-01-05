import requests
import main
import os
from logger import log

def get_all_char():
    try:
        res = requests.get(main.SWAPI_TECH_URL)
        data = res.json()
        results = data["results"]

        next_url = data["next"]
        while next_url and len(results) <= 30:
            res = requests.get(next_url)
            data = res.json()
            results.extend(data["results"])
            next_url = data["next"]

        results = results[:29]

        if not os.path.exists(main.file_path):
            with open(main.file_path, "a") as csv_file:
                csv_file.write("uid,name,url\n")
                for person in results:
                    uid = int(float(person["uid"]))
                    name = person["name"]
                    url = person["url"]
                    csv_file.write(f"{uid},{name},{url}\n")
                    
        log.info("Got all characters from SWAPI Tech")

        return results
    
    except Exception as e:
        log.info("Error on getting all characters from SWAPI Tech")
        return e