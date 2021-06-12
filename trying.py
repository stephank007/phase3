import pandas as pd
import plotly.graph_objects as go
import plotly.offline as pyo
import numpy as np
import os
import codecs
import yaml

print(os.getcwd())
with codecs.open('./config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
fn = os.path.join(path, './_out/stat-file.xlsx')
fo = os.path.join(path, './_out/stat-file.html')
f1 = os.path.join(path, './_out/stat-file-out.xlsx')

writer   = pd.ExcelWriter(f1, engine='xlsxwriter')
workbook = writer.book

df = pd.read_excel(fn, sheet_name='Sheet1')
df['month'] = pd.to_datetime(df['Finish_Date']).dt.strftime('%Y-%m')

df_g = df.groupby(['month']).agg({
    'total' : 'sum',
    'open'  : 'sum',
    'closed': 'sum'
})
df_g.reset_index(inplace=True)
gt = df_g['total'].sum()
df_g['month-label'] = pd.to_datetime(df_g['month'], format='%Y-%m').dt.strftime('%b-%Y')
df_g['% open']      = ( df_g['open'] / df_g['total'] )   * 100
df_g['% closed']    = ( df_g['closed'] / df_g['total'] ) * 100
df_g['width']       = ( df_g['total'] / gt )             * 100

df_g.to_excel(writer, sheet_name='Sheet1')
writer.save()

labels = df_g['month-label']
widths = np.array(df_g['width'])

data = {
    "closed"  : df_g['% closed'].apply(np.ceil),
    "open"    : df_g['% open'].apply(np.floor),
}
colors = {
    'closed': 'lightslategray',
    'open'  : 'crimson'
}
customdata = np.transpose([labels, data['closed']]),
print('length --> ', len(customdata))
df_x = pd.DataFrame(customdata)

fig = go.Figure()
for key in data:
    fig.add_trace(go.Bar(
        name=key,
        y=data[key],
        x=np.cumsum(widths)-widths,
        #x=labels.to_list(),
        width=widths,
        marker_color=colors[key],
        offset=0,
        customdata=np.transpose([labels, data[key]]),
        text='from total of '    + df_g['total'].astype(str) + ' requirements' +
             '<br>open/closed: ' + df_g['open'].astype(str)  + '/' + df_g['closed'].astype(str),
        textfont_color='lightsalmon',
        texttemplate="%{y}%",
        textposition="inside",
        textangle=0,
        hovertemplate="<br>".join([
            "%{customdata[0]}",
            key + ": %{y}%" ,
            "%{text}"
        ]),
    ))

fig.update_xaxes(
    tickvals=np.cumsum(widths)-widths/2,
    #ticktext= ["%s<br>%d" % (l, w) for l, w in zip(labels, widths)]
    ticktext=["%s" % (l) for l, w in zip(labels, widths)],
)

fig.update_xaxes(range=[0, 100])
fig.update_yaxes(range=[0, 100])

fig.update_layout(
    hoverlabel=dict(
        bgcolor='white',
        font_size=16,
        font_family='Courier New, monospace'
    )
)

fig.update_layout(
    title_text="% open vs. closed - Marimekko Chart",
    barmode="stack",
    uniformtext=dict(mode="hide", minsize=10),
    xaxis={'categoryorder': 'category ascending'},
    xaxis_tickangle=-45,
    height=600
)
pyo.plot(fig, filename=fo)
