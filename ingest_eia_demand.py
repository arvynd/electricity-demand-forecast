import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from loguru import logger


def get_all_data(params: dict, base_url: str) -> list:
    """Get all data from the EIA API by paginating through the results."""
    all_data = []
    offset = 0
    while True:
        params["offset"] = offset
        response = requests.get(base_url, params=params, timeout=60)
        logger.info(response.raise_for_status())
        data = response.json()
        new_data = data.get("response", {}).get("data", [])
        if not new_data:
            break
        all_data.extend(new_data)
        offset += len(new_data)
    return all_data


def main():
    """Main function to ingest EIA demand data."""
    # Environment
    load_dotenv()

    EIA_API_KEY = os.getenv("EIA_API_KEY")
    if not EIA_API_KEY:
        raise RuntimeError("EIA_API_KEY not set")

    BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

    # Snapshot semantics
    SNAPSHOT_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    CUTOFF_TS = "2022-12-31T23:59:00Z"

    # Directory setup.
    BASE_DIR = "data"
    RAW_DIR = f"{BASE_DIR}/raw/eia_api/snapshot_date={SNAPSHOT_DATE}/cutoff={CUTOFF_TS}"
    PARSED_DIR = (
        f"{BASE_DIR}/parsed/demand/snapshot_date={SNAPSHOT_DATE}/cutoff={CUTOFF_TS}"
    )

    # In production, you might want to check if the data already exists and
    # skip this job if it does.
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PARSED_DIR, exist_ok=True)

    # API parameters
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "LDWP",
        "facets[type][]": "D",
        "start": "2022-01-01T00",
        "end": "2023-01-01T00",
        "length": 5000,
    }

    # API pull
    raw_data = get_all_data(params, BASE_URL)

    # Persist RAW snapshot.
    raw_path = f"{RAW_DIR}/response.json"
    with open(raw_path, "w") as f:
        # In production, you might write this to a cloud storage bucket like S3
        # instead of a local file.
        json.dump(raw_data, f, indent=2)

    metadata = {
        "snapshot_date": SNAPSHOT_DATE,
        "cutoff_timestamp": CUTOFF_TS,
        "api_endpoint": BASE_URL,
        "api_params": {
            k: v for k, v in params.items() if k != "api_key"
        },  # Don't save secrets
        "row_count": len(raw_data),
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    with open(f"{RAW_DIR}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # Parse into DataFrame.
    data_df = pd.DataFrame(raw_data)

    # Canonical typing / normalization.
    # In production, you'd want to have robust error handling here.
    # For example, what happens if the 'period' column has an unexpected format?
    data_df["period"] = pd.to_datetime(data_df["period"], utc=True)
    data_df["value"] = pd.to_numeric(data_df["value"], errors="coerce")

    # Persist parsed dataset.
    parquet_path = f"{PARSED_DIR}/demand.parquet"
    csv_path = f"{PARSED_DIR}/demand.csv"
    data_df.to_parquet(parquet_path, index=False)
    data_df.to_csv(csv_path, index=False)

    schema_path = f"{PARSED_DIR}/schema.json"
    with open(schema_path, "w") as f:
        json.dump(
            {col: str(dtype) for col, dtype in data_df.dtypes.items()},
            f,
            indent=2,
        )

    print("Snapshot completed successfully")


if __name__ == "__main__":
    main()
