import os
import requests
import pandas as pd
import re
import csv
import main
from logger import log


def get_homeworld():
    try:
        df = pd.read_csv(main.file_path_details)
        homeworld_urls = df["homeworld"]

        patt = re.compile(r"^https?://.+/planets/\d+/?$")
        file_exists = os.path.exists(main.file_path_homeworld)

        results = []

        with open(main.file_path_homeworld, "a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)

            if not file_exists:
                writer.writerow([
                    "homeworld_name", "rotation_period", "orbital_period",
                    "diameter", "climate", "gravity",
                    "terrain", "surface_water", "population"
                ])

            for url in homeworld_urls:
                if not isinstance(url, str) or not patt.match(url):
                    row = {
                        "homeworld_name": None,
                        "rotation_period": None,
                        "orbital_period": None,
                        "diameter": None,
                        "climate": None,
                        "gravity": None,
                        "terrain": None,
                        "surface_water": None,
                        "population": None,
                    }
                else:
                    res = requests.get(url)
                    res.raise_for_status()
                    data = res.json()

                    
                    planet = data.get("result", {}).get("properties", data)

                    row = {
                        "homeworld_name": planet.get("name"),
                        "rotation_period": planet.get("rotation_period"),
                        "orbital_period": planet.get("orbital_period"),
                        "diameter": planet.get("diameter"),
                        "climate": planet.get("climate"),
                        "gravity": planet.get("gravity"),
                        "terrain": planet.get("terrain"),
                        "surface_water": planet.get("surface_water"),
                        "population": planet.get("population"),
                    }

                
                writer.writerow(row.values())

                
                results.append(row)

        return {
            "count": len(results),
            "homeworlds": results
        }

    except Exception as e:
        log.exception("Error on getting all homeworlds from SWAPI Homeworld")
        raise RuntimeError(str(e))