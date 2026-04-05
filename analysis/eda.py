# %%
import pandas as pd
demand = pd.read_parquet('../data/parsed/demand/snapshot_date=2026-04-05/demand.parquet')
demand
# %%
import plotly.express as px

fig = px.line(demand, x="period", y="value", color="duoarea")
fig.show()
# %%
