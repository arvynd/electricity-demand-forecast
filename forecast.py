from loguru import logger
from pathlib import Path

import pandas as pd

from models import chronos_forecast, seasonal_naive_forecast, select_chronos_params


def read_latest_demand(base_dir: str = "data/parsed/demand") -> pd.DataFrame:
    files = sorted(Path(base_dir).glob("snapshot_date=*/demand.parquet"))
    if not files:
        raise FileNotFoundError(f"No demand parquet files found in {base_dir}")
    latest = files[-1]
    logger.info("Reading demand data from {}", latest)
    return pd.read_parquet(latest)


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: validate required columns exist before rename (period, duoarea, value)
    # TODO: validate ds is datetime, y is numeric, no nulls, no duplicate (ds, unique_id) pairs
    df = df.rename(columns={"period": "ds", "duoarea": "unique_id", "value": "y"})
    df = df[["ds", "unique_id", "y"]]
    # TODO: sort by unique_id, ds to ensure chronological order for both models
    return df


def load_data() -> pd.DataFrame:
    df = read_latest_demand()
    return prepare_data(df)


def run_seasonal_naive(df: pd.DataFrame):
    logger.info("Running seasonal naive forecast")
    # TODO: validate each unique_id has >= 52 data points before fitting
    # TODO: h=12 is hardcoded — centralise prediction horizon with run_chronos
    return seasonal_naive_forecast(
        df,
        freq="W",
        seasonal_period=52,
        h=12,
    )


def run_chronos(df: pd.DataFrame):
    logger.info("Running chronos forecast")
    pipeline = select_chronos_params("amazon/chronos-2")
    # TODO: prediction_length=12 is hardcoded — centralise with run_seasonal_naive
    return chronos_forecast(
        df=df,
        pipeline=pipeline,
        target="y",
        id_column="unique_id",
        timestamp_column="ds",
        prediction_length=12,
    )


def run_forecast():
    logger.info("Starting forecast pipeline")
    df = load_data()
    # TODO: capture and persist forecast outputs (e.g. write to parquet)
    run_seasonal_naive(df)
    run_chronos(df)
    logger.info("Forecast pipeline complete")


if __name__ == "__main__":
    run_forecast()
