import os
import codecs
import pandas as pd
import numpy as np
import warnings as warning
import yaml
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table as dt
import plotly.offline as pyo

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

    total = pd.Series(
        dict(
            id='total',
            parent='',
            value=df[value_column].sum(),
            color=df[color_columns[1]].sum() / df[color_columns[0]].sum()
        )
    )
    df_all_trees = df_all_trees.append(total, ignore_index=True)
    return df_all_trees

def get_trace(figure=go.Figure(), df=pd.DataFrame(), group=str):
    df = df.groupby('rule').get_group(group).groupby(by=['domain_id', 'process_id']).sum()[
        ['open_value', 'value']].reset_index()
    levels = ['process_id', 'domain_id']  # levels used for the hierarchical chart

    color_columns = ['value', 'open_value']
    value_column = 'open_value'
    average_score = (df['value'].sum() / df['open_value'].sum())
    df_trees = build_hierarchical_dataframe(df, levels, value_column, color_columns)
    df_trees.fillna(0, inplace=True)
    df_trees['value'] = df_trees['value'].astype(int)

    figure.add_trace(
        go.Sunburst(
            labels=df_trees['id'],
            parents=df_trees['parent'],
            values=df_trees['value'],
            branchvalues='total',
            marker=dict(
                colors=df_trees['color'],
                colorscale='amp',
                cmid=average_score
            ),
            hovertemplate='<b> %{label} </b><br>total: %{value}<br>closed: %{color:.0f}',
            name=group,
        ),
    )
    return figure

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125 * '=')
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
fn = os.path.join(path, config['config'].get('gantt'))
fo1 = os.path.join('./assets/data/_out/1.html')
fo2 = os.path.join('./assets/data/_out/2.html')

df_raw = pd.read_excel(fn, sheet_name='raw-dev')
df_raw['open_value'] = df_raw['weight'] * df_raw['Duration']

""" INT """
fig = go.Figure()
fig = get_trace(fig, df=df_raw, group='SAP')

fig.update_traces(insidetextorientation='radial')
fig.update_layout(
    uniformtext=dict(minsize=8, mode='hide'),
    title='INT'
)
pyo.plot(fig, filename=fo1)

""" SAP """
fig = go.Figure()
fig = get_trace(fig, df=df_raw, group='INT')
fig.update_traces(insidetextorientation='radial')
fig.update_layout(
    uniformtext=dict(minsize=8, mode='hide'),
    title='SAP'
)
pyo.plot(fig, filename=fo2)

