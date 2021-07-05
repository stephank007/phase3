import os
import codecs
import pandas as pd
import numpy as np
import time
import warnings as warning
import yaml

import plotly.express as px
import plotly.offline as pyo

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')

start_s = time.time()

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
rs   = os.path.join(path, config['config'].get('risk_table'))
h_rs = os.path.join(path, config['config'].get('risk-out') )

df_risk   = pd.read_excel(rs, sheet_name='רשימת סיכונים')
df_risk = df_risk[df_risk['risk'] > 1]
h_df_risk = pd.read_excel(h_rs, sheet_name='risk')
h_df_risk = h_df_risk[h_df_risk['risk'] > 1]

h_df_risk['indexDT'] = pd.to_datetime(h_df_risk['d_index'], format='%Y-%m-%d').dt.strftime('%Y-%B').astype(str)
h_df_risk['indexDT'] = h_df_risk['indexDT'].apply(lambda x: x[0:8])
df_risk['indexDT']   = 'today'

df_risk   =   df_risk[['risk', 'Name', 'severity', 'probability', 'indexDT']]
h_df_risk = h_df_risk[['risk', 'Name', 'severity', 'probability', 'indexDT']]

df_risk = pd.concat([df_risk, h_df_risk], ignore_index=True)

g_range = np.arange( 0,  9, 1)
y_range = np.arange( 9, 10, 1)
r_range = np.arange(10, 26, 1)

df_risk['severity'] = df_risk['risk'].apply(
    lambda x:
        'g' if x in g_range else
        'y' if x in y_range else
        'r' if x in r_range else None
)

df_g = df_risk.groupby(['indexDT', 'severity']).size()
df_g = df_g.to_frame()
df_g.reset_index(inplace=True)

def r_stacked_fig():
    fig = px.bar(
        df_g,
        x='indexDT',
        y=0,
        color='severity',
        color_discrete_sequence=['rgb(165, 0, 38)', 'rgb(247, 247, 5)', 'rgb(56, 158, 39)'],
        text=0
    )

    fig.update_traces(
        textposition='inside',
        textfont_size=32
    )

    fig.update_layout(
        title='Risk Management',
        xaxis_title='Dates',
        yaxis_title='# of Risks',
        font=dict(
            family='Courier New, monospace',
            size=18,
            color="RebeccaPurple"
        ),
        paper_bgcolor = '#9ab3a0',
        plot_bgcolor = '#c7d6cb',
        height = 600,
    )
    # pyo.plot(fig, filename='r_stacked.html')
    return fig
