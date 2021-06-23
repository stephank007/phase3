import os
import codecs
import time
import yaml
import pandas as pd
import json

import plotly.express as px
import plotly.offline as pyo

import dash
import flask

import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from flask import Flask
from dash.dependencies import Input, Output, State

print(125 * '=')
start_s = time.time()
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

publish = config['execution_params'].get('publish')
baseline = True if config['execution_params'].get('baseline') else False

today    = pd.to_datetime('today')
deadline = pd.to_datetime('2020/10/31', format='%Y/%m/%d')
x_start  = pd.to_datetime('2020/08/01', format='%Y/%m/%d')

path = config['config'].get('path')

fn = os.path.join(path,  config['config'].get('gantt'))

df = pd.read_excel(fn, sheet_name='gantt-data')
df_bi = pd.read_excel(fn, sheet_name='raw-gantt-data')
df_bi['Start_Date']  = pd.to_datetime(df_bi['Start_Date'], format='%d %B %Y %H:%M')
df_bi['Finish_Date'] = pd.to_datetime(df_bi['Finish_Date'], format='%d %B %Y %H:%M')

fig = px.timeline(
    df,
    x_start='Start_Date',
    x_end='Finish_Date',
    y='process_id',
    color='m_task',
    color_discrete_sequence=px.colors.qualitative.Pastel,
    hover_name='process_id',
    hover_data={
        'process_id': False,
        'm_task'    : True,
        'Start_Date': True
    },
    title='Gantt Report: ' + today.strftime('%d/%m/%Y'),
    category_orders={
        'm_task': ['BP', 'DEV', 'Test', 'Prod']
    },
    height=800
)

for ser in fig['data']:
    c = ser['customdata']
    ser['hovertemplate'] = \
        '<b>%{hovertext}</b><br><br>step  = %{customdata[1]}<br>start = %{customdata[2]|%Y-%m-%d}<br>' \
        'finish= %{x|%Y-%m-%d}<extra></extra>'

server = Flask(__name__)
app = dash.Dash(
    server=server,
    assets_folder=config['config'].get('assets'),
    suppress_callback_exceptions=True
)

body = dbc.Container([
    html.Hr(),
    dbc.Row([
        dbc.Col(dcc.Graph(figure=fig, id='gantt-chart'), width={'size': 10}),
    ]),
    dbc.Row([
        dbc.Col(
            html.Pre(
                id='structure',
                style={
                    'border': 'thin lightgrey solid',
                    'overflowY': 'scroll',
                    'height': '275px'
                }
            )
        )
    ]),
    html.Hr(),
], fluid=True)

app.layout = html.Div([body])

@app.callback(
    Output('structure', 'children'),
    Input('gantt-chart', 'figure'))
def display_hover_data(fig_json):
    return json.dumps(fig_json, indent=2)

if __name__ == '__main__':
    app.run_server(debug=True)
