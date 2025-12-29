import os
import requests
import pandas as pd
import re
import csv
import main

def get_homeworld():
    try:
        df = pd.read_csv(main.file_path_details)
        homeworld_url = df["homeworld"]
        patt = re.compile(r"^https?://.+/planets/\d+/?$")
        
        file_exists = os.path.exists(main.file_path_homeworld)

        with open(main.file_path_homeworld, "a", newline="", encoding="utf-8") as csv_file:
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