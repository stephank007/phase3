import os
import codecs
import pandas as pd
import numpy as np
import warnings as warning
import yaml
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as pyo

pc = '% Complete'

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


#print(df.head())
def build_hierarchical_dataframe(df, levels, value_column, color_columns=None):
    """
    Build a hierarchy of levels for Sunburst or Treemap charts.

    Levels are given starting from the bottom to the top of the hierarchy,
    ie the last level corresponds to the root.
    """
    df_all_trees = pd.DataFrame(columns=['id', 'parent', 'value', 'color'])
    for i, level in enumerate(levels):
        df_tree = pd.DataFrame(columns=['id', 'parent', 'value', 'color'])
        dfg = df.groupby(levels[i:]).sum()
        dfg = dfg.reset_index()
        df_tree['id'] = dfg[level].copy()
        if i < len(levels) - 1:
            df_tree['parent'] = dfg[levels[i+1]].copy()
        else:
            df_tree['parent'] = 'total'
        df_tree['value'] = dfg[value_column]
        df_tree['color'] = dfg[color_columns[0]] / dfg[color_columns[1]]
        df_all_trees = df_all_trees.append(df_tree, ignore_index=True)
    total = pd.Series(dict(id='total', parent='',
                           value=df[value_column].sum(),
                           color=df[color_columns[0]].sum() / df[color_columns[1]].sum()))
    df_all_trees = df_all_trees.append(total, ignore_index=True)
    return df_all_trees

df['key'] = df['domain_id'] + '--' + df['process_id']
df = df.groupby(by=['rule', 'key']).sum()[['old_budget', 'value']].reset_index().rename(columns={'value': 'value_x'})

levels = ['key' , 'rule']  # levels used for the hierarchical chart
color_columns = ['old_budget', 'value_x']
value_column = 'old_budget'
average_score = df['value_x'].sum() / df['old_budget'].sum()
df_all_trees = build_hierarchical_dataframe(df, levels, value_column, color_columns)
df_all_trees.fillna(0, inplace=True)

#
df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/sales_success.csv')
print(df.head())
#
levels = ['salesperson', 'county', 'region'] # levels used for the hierarchical chart
color_columns = ['sales', 'calls']
value_column = 'calls'

# average_score = df['sales'].sum() / df['calls'].sum()
# df_all_trees = build_hierarchical_dataframe(df, levels, value_column, color_columns)
##############################################################

fig = go.Figure()
fig.add_trace(go.Sunburst(
    labels=df_all_trees['id'],
    parents=df_all_trees['parent'],
    values=df_all_trees['value'],
    branchvalues='total',
    marker=dict(
        colors=df_all_trees['color'],
        colorscale='RdBu',
        cmid=average_score),
    hovertemplate='<b>%{label} </b> <br> value: %{value_x}<br> value: %{color:.2f}',
    name=''
    )
)

fig.update_traces(insidetextorientation='radial')
fig.update_layout(
    title="process weight distribution",
    width=1000,
    height=1000,
    uniformtext=dict(minsize=10, mode='hide')
)
pyo.plot(fig, filename=fo)
