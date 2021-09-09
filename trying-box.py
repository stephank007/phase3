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

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125 * '=')

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
f3 = os.path.join(path, config['config'].get('gantt'))
fo = os.path.join(path, '_out/xyz.html')

df = pd.read_excel(f3, sheet_name='raw-dev')
df['value'] = df['Duration'] * df['weight']
df['month'] = df['Finish_Date'].dt.strftime('%Y-%m')
df['process'] = df['process'].apply(lambda x: f'{x:0>5}')
df = df[( ~df['process'].isin(['10.08']) ) & ( ~df['xRN'].isin(['01.15.IMP.01', '02.02.IMP.01', '01.11.IMP.05', '01.11.SAP.03']) )]

fig = px.box(df, x='month', y='value', color='rule', notched=True, color_discrete_sequence=['indianred', 'lightseagreen'])
fig.update_traces(quartilemethod="exclusive")
fig.update_layout(
    title={
        'text': 'מדד תפוקות',
        'y': 0.90,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    },
    title_font_size=36,
    # xaxis_title='תאריך דיווח',
    # yaxis_title='תאריכי אבני הדרך',
    font=dict(
        family='Times Roman',
        size=14,
        color="RebeccaPurple"
    ),
    # height=400,
    # paper_bgcolor = '#9ab3a0',
    paper_bgcolor='#f8f5f0',
    plot_bgcolor='#c7d6cb'
)

pyo.plot(fig, filename=fo)
