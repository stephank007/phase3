
import os
import codecs
import pandas as pd
import numpy as np
import time
import warnings as warning
import yaml

import plotly.figure_factory as ff
import plotly.offline as pyo

# pd.options.mode.chained_assignment = None
# isinstance(row.due_date, pd._libs.tslibs.nattype.NaTType)
# pd.bdate_range(start=start_d, end='20/10/2020', weekmask=weekmask, holidays=holidays, freq='C')
pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')

start_s = time.time()

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
fn = os.path.join(path, config['config'].get('risk_map'))
fo = os.path.join(path, 'risk_out.xlsx')
rs = os.path.join(path, config['config'].get('risk'))

z_text = np.empty((5, 5), dtype=int)
z_text[:] = 0
z_text = [
     ['', '', '', '', ''],
     ['', '', '', '', ''],
     ['', '', '', '', ''],
     ['', '', '', '', ''],
     ['', '', '', '', '']
]

# for idx, x in np.ndenumerate(z_text):
#    z_text[idx] = 0

z = [
    [5, 10, 15, 20, 25],
    [4, 8, 12, 16, 20],
    [3, 6, 9, 12, 15],
    [2, 4, 6, 8, 10],
    [1, 2, 3, 4, 5]
]
df_risk = pd.read_excel(rs, sheet_name='רשימת סיכונים')
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
#    print('count_x: {} severity: {} probability: {} z_text: {}'.format(row.count_x, row.severity, row.probability, z_text[row.severity, row.probability]))

z_text = np.flip(z_text, 0)
#z_text = np.array(map(str, z_text))

x = [1, 2, 3, 4, 5]
y = [5, 4, 3, 2, 1]
font_colors = ['black']

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
    )
)
fig.show()

# df_sm = pd.melt(df, id_vars=0, value_vars=df.iloc[:, 1:6], value_name='risk')
# df_sm = pd.DataFrame(df_sm['risk'].unique())
# df_sm = df_sm.rename(columns={0: 'risk'})
# df_sm['key'] = df_sm.risk
# df_sm.set_index('key', inplace=True)
# df_sm.sort_index(inplace=True)
#
# g_range = np.arange( 0,  9, 1)
# y_range = np.arange( 9, 13, 1)
# r_range = np.arange(13, 26, 1)
# df_sm['severity'] = df_sm['risk'].apply(
#     lambda x:
#         'g' if x in g_range else
#         'y' if x in y_range else
#         'r' if x in r_range else None
# )

#fig = px.imshow(df_sm)
#pyo.plot(fig, filename=fo)
# z_text = [
#     ['', '', 4, 12, 6],
#     ['', 8, '', 17, ''],
#     ['', 7, 11, '', 2],
#     ['', 6, '', 17, ''],
#     ['', 5, '', 17, '']
# ]
