import os
import codecs
import yaml
import pandas as pd
import plotly.graph_objects as go
import mongo_services as ms
import plotly.offline as pyo

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


path = config['config'].get('path')
fn = os.path.join(config['config'].get('path'), config['config'].get('rule'))
df = pd.read_excel(fn, sheet_name='mta')
db = ms.mongo_connect()

mta_map = config['MTA']

collection = ms.get_collection(db, 'BI')
df_bi      = ms.retrieve_all(collection)
print(df_bi.shape)
df_bi['mta']      = df_bi.Task_Name.map(mta_map)
df_bi             = df_bi[['mta', 'Finish_Date', 'd_index']]
df_bi['d_index']  = pd.to_datetime(df_bi['d_index'])
df_pdr            = df_bi[df_bi['mta'] == 'PDR'].reset_index()
df_pdr['yyyy-mm'] = df_pdr['d_index'].map(lambda x: x.strftime('%Y-%m'))

df_pdr =  df_pdr.groupby(['yyyy-mm']).agg(
    {
        'd_index'    : 'max',
        'Finish_Date': 'min'
    }
)
df['yyyy-mm'] = df['x'].map(lambda x: pd.to_datetime(x).strftime('%Y-%m'))
df['PDR']     = df['PDR'].dt.strftime('%Y-%m-%d')
df.set_index('yyyy-mm', inplace=True)
df.loc[df_pdr.index, 'PDR'] = pd.to_datetime(df_pdr['Finish_Date']).dt.strftime('%Y-%m-%d')

writer   = pd.ExcelWriter(os.path.join(path, 'mta_out.xlsx'), engine='xlsxwriter')
workbook = writer.book
df.to_excel(writer, sheet_name='Sheet1')
writer.save()

fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=df['line'],
        y=df['y'],
        mode='lines',
        showlegend=False
    )
)

milestones = ['ARO', 'SRR', 'PDR']
for ms in milestones:
    fig.add_trace(
        go.Scatter(
            x = df['x'],
            y = df[ms],
            name = ms,
            mode = 'lines+markers',
            marker={
                'size' : 10,
                'symbol' : 'square'
            }
        )
    )

fn = os.path.join(config['config'].get('web'), 'mta.html')
pyo.plot(fig, filename=fn)
