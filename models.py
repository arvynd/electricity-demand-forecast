import pandas as pd
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
    # TODO: widen type hint to BaseChronosPipeline — Chronos2Pipeline is too narrow
    pipeline: Chronos2Pipeline,
    target: str,
    id_column: str,
    timestamp_column: str,
    prediction_length: int,
    # TODO: mutable default argument anti-pattern — replace with None and assign inside body
    quantile_levels: list[float] = [0.1, 0.5, 0.9],
):
    return pipeline.predict_df(
        df,
        prediction_length=prediction_length,
        quantile_levels=quantile_levels,
        id_column=id_column,
        timestamp_column=timestamp_column,
        target=target,
    )


def select_chronos_params(model: str, device_map: str = "cuda"):
    # TODO: default device_map="cuda" crashes on CPU-only machines — detect via torch.cuda.is_available()
    pipeline: Chronos2Pipeline = BaseChronosPipeline.from_pretrained(
        model, device_map=device_map
    )
    return pipeline
