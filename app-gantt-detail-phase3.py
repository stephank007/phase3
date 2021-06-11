import os
import codecs
import time
import yaml
import pandas as pd

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

server = Flask(__name__)
app = dash.Dash(
    server=server,
    assets_folder=config['config'].get('assets'),
    suppress_callback_exceptions=True
)

body = dbc.Container([
    html.Hr(),
    dbc.Row([
        dbc.Col(),
        dbc.Col(dcc.Graph(figure=fig, id='gantt-chart'), width={'size': 10}),
        dbc.Col(
            dbc.Card(id="my_card", children=[
                dbc.CardBody(id='click-data')
            ], color='secondary', inverse=False), width={'size': 5, 'offset': 0}
        )
    ]),
    html.Hr(),
], fluid=True)

app.layout = html.Div([body])

@app.callback(
    Output('click-data', 'children'),
    Input('gantt-chart', 'clickData'))
def display_hover_data(clickData):
    t1 = None
    if clickData is not None:
        t1 = clickData.get('points')[0]
        t1 = t1.get('customdata')
        p_id = t1[0]
        step = t1[1]
        print(p_id, step)
        dff = df_bi[( df_bi['process_id'] == p_id ) & ( df_bi['m_task'] == step )]
        print(dff.shape)
    if t1 is not None:
        new_card_content = [dbc.ListGroupItem(x, style={'textAlign': 'right'}) for x in t1]
        new_card_body = [html.H4("רשימת פגישות", className="card-title", style={'textAlign': 'center'})] + new_card_content
    else:
        new_card_content = [dbc.ListGroupItem('טרם עברתם על הנקודות שבגרף...', style={'textAlign': 'right'})]
        new_card_body = [html.H4("רשימת פגישות", className="card-title", style={'textAlign': 'center'})] + new_card_content

    return new_card_body

if __name__ == '__main__':
    app.run_server(debug=True)
