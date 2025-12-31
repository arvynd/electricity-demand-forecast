import requests
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


EIA_API_KEY = os.getenv("EIA_API_KEY")
if not EIA_API_KEY:
    raise RuntimeError("EIA_API_KEY not set")

base_url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

params = {
    "api_key": EIA_API_KEY,
    "data[0]": "value",
    "facets[type][]": "D",
    "length": 5000,
    "start": "2023-01-01",
    "end": "2023-01-02",
}

all_rows = []
offset = 0
page_size = 5000

while True:
    params["offset"] = offset

    response = requests.get(
        "https://api.eia.gov/v2/electricity/rto/region-data/data/",
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    batch = response.json()["response"]["data"]
    if not batch:
        break

    all_rows.extend(batch)

    if len(batch) < page_size:
        break

    offset += page_size

df = pd.DataFrame(all_rows)
