from typing import Optional

import pandas as pd
import torch
from chronos import BaseChronosPipeline, Chronos2Pipeline
from statsforecast import StatsForecast
from statsforecast.models import SeasonalNaive


def seasonal_naive_forecast(
    df: pd.DataFrame, freq: str = "W", seasonal_period: int = 52, h: int = 24
):
    sf = StatsForecast(
        models=[SeasonalNaive(seasonal_period)],
        freq=freq,
    )
    sf.fit(df=df)
    forecast = sf.predict(h=h)
    return forecast


def chronos_forecast(
    df: pd.DataFrame,
    pipeline: BaseChronosPipeline,
    target: str,
    id_column: str,
    timestamp_column: str,
    prediction_length: int,
    quantile_levels: Optional[list[float]] = None
):
    if quantile_levels is None:
        quantile_levels = [0.1, 0.5, 0.9]
    return pipeline.predict_df(
        df,
        prediction_length=prediction_length,
        quantile_levels=quantile_levels,
        id_column=id_column,
        timestamp_column=timestamp_column,
        target=target,
    )


def select_chronos_params(model: str, device_map: str = "cpu"):
    if device_map == "cuda" and not torch.cuda.is_available():
        device_map = "cpu"
    pipeline: Chronos2Pipeline = BaseChronosPipeline.from_pretrained(
        model, device_map=device_map
    )
    return pipeline
