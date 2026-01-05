import requests
import os
import main
import csv
from logger import log

def get_all_details():
    try:
        res = requests.get(main.SWAPI_DETAILS_URL)
        data = res.json()

        result = data[:29]

        file_exists = os.path.exists(main.file_path_details)

        with open(main.file_path_details, "a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)

            if not file_exists:
                writer.writerow([
                    "name",
                    "height",
                    "mass",
                    "hair_color",
                    "skin_color",
                    "eye_color",
                    "birth_year",
                    "gender",
                    "homeworld"
                ])

            for details in result:
                # defensive extraction (same idea as homeworld)
                writer.writerow([
                    details.get("name"),
                    details.get("height"),
                    details.get("mass"),
                    details.get("hair_color"),
                    details.get("skin_color"),
                    details.get("eye_color"),
                    details.get("birth_year"),
                    details.get("gender"),
                    details.get("homeworld")
                ])
        log.info("Got all characters  detalis from SWAPI Info")
        return {"status": "details saved successfully"}

    except Exception as e:
        log.info("Error on getting all details from SWAPI Info")
        return e