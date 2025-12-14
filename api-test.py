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
}

response = requests.get(base_url, params=params, timeout=10)
response.raise_for_status()

data = response.json()

df = pd.DataFrame(data["response"]["data"])
