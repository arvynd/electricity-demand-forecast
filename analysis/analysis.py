# %%
import pandas as pd
demand = pd.read_parquet('../data/parsed/demand/snapshot_date=2026-04-18/demand.parquet')
demand
# %%
from statsforecast import StatsForecast
from statsforecast.models import SeasonalNaive
# %%
sf = StatsForecast(
    models=[SeasonalNaive(52)],
    freq="W",
)
# %%
demand = demand.rename(columns={"period" : "ds", "value" : "y", "duoarea" : "unique_id"})
# %%
demand = demand[["unique_id", "ds", "y"]]
# %%
sf.fit(df=demand)
# %%
forecast = sf.predict(h=24)
# %%
forecast.loc[:, "Series"] = "Seasonal Naive"
# %%
forecast.rename(columns={"SeasonalNaive" : "y"}, inplace=True)
# %%
demand.loc[:,"Series"] = "Actuals"
# %%
demand
# %%
final_df = pd.concat([demand, forecast])
# %%
final_df.columns
# %%
import plotly.express as px
# %%
fig = px.line(
    final_df,
    x="ds",
    y="y",
    color="Series",
    facet_col="unique_id",
    facet_col_wrap=2,
    height=900,
    title="Natural Gas Storage by Region",
    labels={"ds": "Date", "y": "BCF", "unique_id": "Region"},
)

fig.update_layout(showlegend=False)
fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig.show()
# %%
from chronos import BaseChronosPipeline, Chronos2Pipeline
# %%
pipeline: Chronos2Pipeline = BaseChronosPipeline.from_pretrained("amazon/chronos-2", device_map="cuda")
# %%
# Energy price forecasting configuration
target = "y"  # Column name containing the values to forecast (energy prices)
prediction_length = 24  # Number of hours to forecast ahead
id_column = "unique_id"  # Column identifying different time series (countries/regions)
timestamp_column = "ds"  # Column containing datetime information
# %%
energy_pred_df = pipeline.predict_df(
    demand,
    # future_df=energy_future_df,
    prediction_length=prediction_length,
    quantile_levels=[0.1, 0.5, 0.9],
    id_column=id_column,
    timestamp_column=timestamp_column,
    target=target,
)
# %%
energy_pred_df
# %%
energy_df = energy_pred_df.rename(columns={"predictions": "y"})
# %%
energy_df = energy_df[["unique_id", "ds", "y"]]
# %%
energy_df.loc[:,"Series"] = "Energy"
# %%
final_df = pd.concat([demand, forecast, energy_df])
# %%
final_df