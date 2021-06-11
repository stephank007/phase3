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
webp = config['config'].get('web')
fn = os.path.join(path, './_out/stat-file.xlsx')
fo = os.path.join(path, './_out/stat-file.html')
f1 = os.path.join(path, './_out/stat-file-out.xlsx')

writer   = pd.ExcelWriter(f1, engine='xlsxwriter')
workbook = writer.book

df = pd.read_excel(fn, sheet_name='Sheet1')
df['month'] = pd.to_datetime(df['Finish_Date']).dt.strftime('%Y-%m')

df_g = df.groupby(['month']).agg({
    'total'    : 'sum',
    'in-dev'   : 'sum',
    'completed': 'sum'
})
df_g.reset_index(inplace=True)
gt = df_g['total'].sum()
df_g['month-label'] = pd.to_datetime(df_g['month'], format='%Y-%m').dt.strftime('%b-%Y')
df_g['% in-dev'] = ( df_g['in-dev'] / df_g['total'] ) * 100
df_g['% completed'] = ( df_g['completed'] / df_g['total'] ) * 100
df_g['width'] = ( df_g['total'] / gt ) * 100

df_g.to_excel(writer, sheet_name='Sheet1')
writer.save()

# labels = ["apples","oranges","pears","bananas"]
labels = df_g['month-label']
#widths = np.array([10,20,20,50])
widths = np.array(df_g['width'])

data = {
    "completed"  : df_g['% completed'].apply(np.ceil),
    "in-dev"     : df_g['% in-dev'].apply(np.floor),
}

fig = go.Figure()
for key in data:
    fig.add_trace(go.Bar(
        name=key,
        y=data[key],
        x=np.cumsum(widths)-widths,
        #x=labels.to_list(),
        width=widths,
        offset=0,
        customdata=np.transpose([labels, data[key]]),
        text='# in-dev: ' + df_g['in-dev'].astype(str) + '<br>' + '# completed: ' + df_g['completed'].astype(str),
        texttemplate="%{y} ",
        textposition="inside",
        textangle=0,
        hovertemplate="<br>".join([
            "%{customdata[0]}",
            key + ": %{y}%",
            "%{text}"
        ]),
    ))

fig.update_xaxes(
    tickvals=np.cumsum(widths)-widths/2,
    #ticktext= ["%s<br>%d" % (l, w) for l, w in zip(labels, widths)]
    ticktext=["%s" % (l) for l, w in zip(labels, widths)]
)

fig.update_xaxes(range=[0,100])
fig.update_yaxes(range=[0,100])

fig.update_layout(
    title_text="% in-progress vs. completed - Marimekko Chart",
    barmode="stack",
    uniformtext=dict(mode="hide", minsize=10),
    xaxis={'categoryorder': 'category ascending'}
)
pyo.plot(fig, filename=fo)
