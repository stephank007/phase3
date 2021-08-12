import os
import codecs
import pandas as pd
import numpy as np
import time
import warnings as warning
import yaml

import plotly.figure_factory as ff
import plotly.offline as pyo

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')

start_s = time.time()

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
rs   = os.path.join(path, config['config'].get('risk_table'))

df_risk = pd.read_excel(rs, sheet_name='רשימת סיכונים')

z_text = np.empty((5, 5), dtype=int)
z_text[:] = 0
z_text = [
     ['', '', '', '', ''],
     ['', '', '', '', ''],
     ['', '', '', '', ''],
     ['', '', '', '', ''],
     ['', '', '', '', '']
]

z = [
    [5, 10, 15, 20, 25],
    [4, 8, 12, 16, 20],
    [3, 6, 9, 12, 15],
    [2, 4, 6, 8, 10],
    [1, 2, 3, 4, 5]
]
df_risk = df_risk[df_risk['risk'] > 1]
df_risk = df_risk[['risk', 'Name', 'severity', 'probability']]
df_risk['severity']    = df_risk['severity']    - 1
df_risk['probability'] = df_risk['probability'] - 1

df_risk = df_risk.groupby(['severity', 'probability']).size()
df_risk = df_risk.to_frame()
df_risk = df_risk.rename(columns={0: 'count_x'})
df_risk.reset_index(inplace=True)

for index, row in df_risk.iterrows():
    x = row.severity
    y = row.probability
    z_text[x][y] = str(row.count_x)

z_text = np.flip(z_text, 0)

x = [1, 2, 3, 4, 5]
y = [5, 4, 3, 2, 1]
font_colors = ['black']


def risk_heatmap():
    fig = ff.create_annotated_heatmap(
        z,
        x=x,
        y=y,
        annotation_text=z_text,
        colorscale=[
            [0.00, 'rgb(56, 158, 39)'],
            [0.32, 'rgb(247, 247, 5)'],
            [0.60, 'rgb(244, 109, 67)'],
            [1.00, 'rgb(165, 0, 38)'],
        ],
        colorbar=dict(
            title='Risk Level'
        ),
        font_colors=font_colors,
        showscale=True,
        hoverinfo='none'
    )

    for i in range(len(fig.layout.annotations)):
        fig.layout.annotations[i].font.size = 32

    fig.update_layout(
        title='Risk Management',
        xaxis_title='Severity',
        yaxis_title='Probability',
        font=dict(
            family='Courier New, monospace',
            size=18,
            color="RebeccaPurple"
        ),
        # paper_bgcolor='#9ab3a0',
        paper_bgcolor='#f8f5f0',
        plot_bgcolor='#c7d6cb',
        height = 600,
    )
    return fig
    # pyo.plot(fig, filename='risk.html')
