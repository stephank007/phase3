import os
import codecs
import pandas as pd
import numpy as np
import warnings as warning
import yaml
import plotly.express as px
import plotly.graph_objects as go
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table as dt
import r_heatmap
import r_stacked
import plotly.offline as pyo

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125 * '=')

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

holidays = []
for d in config['bdays'].get('holidays'):
    holidays.append(pd.to_datetime(d))
weekmask = config['bdays'].get('bdays')
custombday = pd.offsets.CustomBusinessDay(weekmask=weekmask, holidays=holidays)

holidays = pd.to_datetime(config['holidays'])
(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
weekends = (FRI, SAT)

path = config['config'].get('path')
f3 = os.path.join(path, config['config'].get('gantt'))
fo = os.path.join(path, '_out/xyz.html')

df = pd.read_excel(f3, sheet_name='raw-dev')
df['value'] = df['Duration'] * df['weight']

# world_countries_data = pd.read_csv("assets/data/_in/countries of the world.csv")
indian_district_population = pd.read_csv("assets/data/_in/district wise population for year 2001 and 2011.csv")
starbucks_locations = pd.read_csv("assets/data/_in/directory.csv")
#
# starbucks_dist = starbucks_locations.groupby(by=["Country", "State/Province", "City"]).count()[["Store Number"]].rename(
#     columns={"Store Number": "Count"})
#
# starbucks_dist["World"] = "World"
# starbucks_dist = starbucks_dist.reset_index()
# world_countries_data["World"] = "World"
#
# indian_district_population["Country"] = "India"
#
# indian_district_population = df.groupby(by=['domain_id', 'process_id']).sum()[['value']].rename({'value': 'Population'})
# indian_district_population["Country"] = "India"
#
# region_wise_pop = world_countries_data.groupby(by="Region").sum()[["Population"]].reset_index()

world_countries_data = df.groupby(by=['rule', 'domain_id']).sum()[['value']].reset_index().rename(columns={
    'rule'     : 'Region',
    'domain_id': 'Country',
    'value'    : 'Population',
})
world_countries_data['World'] = 'World'
world_countries_data = world_countries_data[world_countries_data['Population'] > 0]

region_wise_pop = world_countries_data.groupby(by="Region").sum()[["Population"]].reset_index()

parents = [""] + ["World"] * region_wise_pop.shape[0] + world_countries_data["Region"].values.tolist()
labels = ["World"] + region_wise_pop["Region"].values.tolist() + world_countries_data["Country"].values.tolist()
values = [world_countries_data["Population"].sum()] + region_wise_pop["Population"].values.tolist() + \
         world_countries_data["Population"].values.tolist()

fig = go.Figure(go.Sunburst(
    parents=parents,
    labels=labels,
    values=values,
))

fig.update_layout(title="World Population Per Country Per Region",
                  width=700, height=700)
pyo.plot(fig, filename=fo)
