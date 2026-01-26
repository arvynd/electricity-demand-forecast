import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
if not EIA_API_KEY:
    raise RuntimeError("EIA_API_KEY not set")

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

params = {
    "api_key": EIA_API_KEY,
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][]": "LDWP",
    "facets[type][]": "D",
    "start": "2022-01-01T00",
    "end": "2022-01-02T00",
    "offset": 0,
    "length": 5000,
}

response = requests.get(BASE_URL, params=params, timeout=60)

# Print the fully resolved URL for debugging
print(response.url)

response.raise_for_status()

data = response.json()
data_df = pd.DataFrame(data["response"]["data"])
data_df.to_csv("data.csv", index=False)
print(data)
