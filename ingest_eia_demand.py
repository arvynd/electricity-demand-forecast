import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

# Environment
load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
if not EIA_API_KEY:
    raise RuntimeError("EIA_API_KEY not set")

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

# Snapshot semantics
SNAPSHOT_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
CUTOFF_TS = "2022-01-01T23:59:00Z"  # Compute dynamically in prod.

# Directory setup.
BASE_DIR = "data"
RAW_DIR = f"{BASE_DIR}/raw/eia_api/snapshot_date={SNAPSHOT_DATE}/cutoff={CUTOFF_TS}"
PARSED_DIR = (
    f"{BASE_DIR}/parsed/demand/snapshot_date={SNAPSHOT_DATE}/cutoff={CUTOFF_TS}"
)

os.makedirs(RAW_DIR, exist_ok=False)
os.makedirs(PARSED_DIR, exist_ok=False)

# API parameters
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

# API pull
response = requests.get(BASE_URL, params=params, timeout=60)
print(response.url)
response.raise_for_status()

raw_json = response.json()

# Persist RAW snapshot.
raw_path = f"{RAW_DIR}/response.json"
with open(raw_path, "w") as f:
    json.dump(raw_json, f, indent=2)

metadata = {
    "snapshot_date": SNAPSHOT_DATE,
    "cutoff_timestamp": CUTOFF_TS,
    "api_endpoint": BASE_URL,
    "api_params": params,
    "row_count": len(raw_json.get("response", {}).get("data", [])),
    "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
}

with open(f"{RAW_DIR}/metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)

# Parse into DataFrame.
data_df = pd.DataFrame(raw_json["response"]["data"])

# Canonical typing / normalization.
data_df["period"] = pd.to_datetime(data_df["period"], utc=True)
data_df["value"] = pd.to_numeric(data_df["value"], errors="coerce")

# Persist parsed dataset.
parquet_path = f"{PARSED_DIR}/demand.parquet"
data_df.to_parquet(parquet_path, index=False)

schema_path = f"{PARSED_DIR}/schema.json"
with open(schema_path, "w") as f:
    json.dump(
        {col: str(dtype) for col, dtype in data_df.dtypes.items()},
        f,
        indent=2,
    )

print("Snapshot completed successfully")
