import os
import codecs
import time
import yaml
import pandas as pd
import plotly.express as px
import plotly.offline as pyo
import plotly.graph_objects as go
import workday as wd

start_s = time.time()
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

publish = config['execution_params'].get('publish')

holidays = []
for d in config['bdays'].get('holidays'):
    holidays.append(pd.to_datetime(d))
weekmask   = config['bdays'].get('bdays')
custombday = pd.offsets.CustomBusinessDay(weekmask=weekmask, holidays=holidays)

holidays = pd.to_datetime(config['holidays'])
(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
weekends = (FRI, SAT)

today    = pd.to_datetime('today')
deadline = pd.to_datetime('2020/10/31', format='%Y/%m/%d')
x_start  = pd.to_datetime('2020/08/01', format='%Y/%m/%d')

history  = config['execution_params'].get('history')
baseline = True if config['execution_params'].get('baseline') else False
if history:
    print('History Execution')
    path = config['history'].get('path')

    fn = os.path.join(path,  config['history'].get('out'))
    fb = os.path.join(path,  config['history'].get('p3BI'))
    f1 = os.path.join(path,  config['history'].get('name'))
    fo = os.path.join(path,  config['history'].get('gantt'))
    lt = os.path.join(path,  config['history'].get('letters'))
    f2 = os.path.join(path,  config['history'].get('domain'))
    hs = os.path.join(path,  config['history'].get('gantt_bl'))
    cs = os.path.join(path,  config['history'].get('c_status'))
else:
    path = config['config'].get('path')

    fn = os.path.join(path,  config['config'].get('out'))
    fb = os.path.join(path,  config['config'].get('p3BI'))
    f1 = os.path.join(path,  config['config'].get('name'))
    fo = os.path.join(path,  config['config'].get('gantt'))
    lt = os.path.join(path,  config['config'].get('letters'))
    hs = os.path.join(config['history'].get('gantt_bl'))
    cs = os.path.join(path,  config['config'].get('c_status'))
    f2 = os.path.join(path,  config['config'].get('name'))

start_d = pd.to_datetime('02/10/2020', format='%d/%m/%Y').strftime('%m/%d/%Y')


df = pd.read_excel(fn, sheet_name='Sheet1')          # p3-out file
bi = pd.read_excel(fb, sheet_name='Task_Table1')     # BI file
rf = pd.read_excel(f2, sheet_name='domain_ref')      # phase-3 - domain sheet
d1 = pd.read_excel(f1, sheet_name='MoM')             # phase-3 - Mom sheet
pr = pd.read_excel(f1, sheet_name='process_ref')     # phase-3 - process_ref sheet
df_letters = pd.read_excel(lt, sheet_name='Sheet1')  # letters file
df_gantt_history = pd.read_excel(hs, sheet_name='Sheet2')  # gantt history
df_letters = df_letters.rename(
    columns={
        'אחראי להתייחסות' : 'responsible',
        'הופץ להתייחסות'  : 'date'
    }
)
df_letters['english_name'] = df_letters.responsible.map(config.get('english_name'))
letters_process_ids = df_letters['process_id'].unique()
df_waiting = pd.DataFrame()
for p in letters_process_ids:
    df_process_id = df_letters[df_letters['process_id'] == p]
    waiting_list = []
    for index, row in df_process_id.iterrows():
        date = str(row.date)
        waiting_list.append(f'{row.english_name:10}' + ' : ' + date)
    df_waiting = df_waiting.append(
        {
            'process_id'   : p,
            'waiting_list' : '<br>'.join(waiting_list)
        },
        ignore_index=True
    )

d1 = d1[['RN', 'pc']]
sd = bi[['RN', 'Start_Date', 'Finish_Date', 'PercentC', 'Duration']]  # selection from the BI file

sd = sd.copy()
sd['duration'] = sd['Duration'].apply(lambda x1: x1.split(' ')[0]).astype(float)
sd['duration'] = sd['duration'].astype(int)

pr = pr[['process_id', 'BP']]

rf.set_index('Name',       inplace=True)
d1.set_index('RN',         inplace=True)
sd.set_index('RN',         inplace=True)
pr.set_index('process_id', inplace=True)

rf = rf.to_dict()
d1 = d1.to_dict()
sd = sd.to_dict()
pr = pr.to_dict()

df['publish']      = df.process_id.map(pr.get('BP'))
df['Name_Ordered'] = df.Name.map(rf.get('Name_Ordered'))
df['pc']           = df.RN.map( d1.get('pc'))
df['Start_Date']   = pd.to_datetime(df.RN.map(sd.get('Start_Date')))
df['Finish_Date']  = pd.to_datetime(df.RN.map(sd.get('Finish_Date')))
df['PercentC']     =                df.RN.map(sd.get('PercentC'))
df['duration']     =                df.RN.map(sd.get('duration'))

df['completed']   = df.apply(lambda x1: x1.duration if x1.PercentC == 1 else 0, axis=1)

''' filter publish and not publish '''
df['row_number'] = df.index
mask             = ~df['Name_Ordered'].isnull()
if publish:
    df = df[mask & (df['publish'] == 'publish')]
else:
    df = df[mask]

for process in df['process_id'].unique():
    s_df = df[(df['process_id'].isin([process]))]
    s_df = s_df.copy()
    if s_df.shape[0] > 0:
        s_df['shift'] = s_df['Finish_Date'].shift(1)
        index_min = s_df.index.min()
        for index, row in s_df.iterrows():
            if index == index_min:
                df.loc[index_min, 'Steps_Completed'] = df.loc[index_min, 'Finish_Date']
                if wd.in_between(0, 1, row.pc):
                    days = row.duration * row.pc
                    df.loc[index_min, 'completed'] = 1
                    df.loc[index_min, 'Steps_Completed'] = wd.workday(row.Start_Date, days=days, weekends=weekends,
                                                                  holidays=holidays)
            elif row.PercentC == 1:
                df.loc[index, 'Steps_Completed'] = row.Finish_Date
                df.loc[index, 'Start_Date']      = row['shift']
                df.loc[index, 'shift']           = row['shift']
            elif wd.in_between(0, 1, row.pc):
                df.loc[index, 'Start_Date']      = row['shift']
                days = row.duration * row.pc
                df.loc[index, 'completed'] = 1
                df.loc[index, 'Steps_Completed'] = wd.workday(row.Start_Date, days=days, weekends=weekends,
                                                              holidays=holidays)
            else:
                df.loc[index, 'Start_Date']      = row['shift']
                df.loc[index, 'shift']           = row['shift']

df_1 = df.copy()
df_1 = df_1[['RN', 'domain_id', 'process_id', 'Name_Ordered', 'Steps_Completed', 'Start_Date', 'Finish_Date',
             'PercentC', 'completed', 'Name', 'shift']]

df_g = df.groupby(['domain_id', 'process_id', 'Name_Ordered']).agg(
    {
        'Start_Date'     : 'min',
        'Finish_Date'    : 'max',
        'Steps_Completed': 'max',
        'completed'      : 'sum'
    }
)
df_x = df_g.groupby(['process_id']).agg(
    {
        'Start_Date'     : 'min',
        'Finish_Date'    : 'max',
        'Steps_Completed': 'max',
        'completed'      : 'sum'
    }
)

df_x['Steps_Completed'] = df_x.apply(lambda x: x.Start_Date if x.completed == 0 else x.Steps_Completed, axis=1)

df_x['x+1'] = df_x['Steps_Completed'].apply(lambda x: x + pd.to_timedelta(1, unit='d'))
df_x['x-1'] = df_x['Steps_Completed'].apply(lambda x: x - pd.to_timedelta(1, unit='d'))

df_g.reset_index(inplace=True)
df_x.reset_index(inplace=True)
df_x['domain'] = df_x['process_id'].apply(lambda x: x[:2])

measures = []
measure_total_bp        = len(df.process_id.unique())
measure_sm_count        = len(df_x[df_x['domain'].isin(['01'])])
measure_scm_count       = len(df_x[df_x['domain'].isin(['02'])])
measure_beyond_deadline = len(df_x[df_x['Finish_Date'] > deadline])
measure_steps_completed = len(df_x[df_x['Finish_Date'] == df_x['Steps_Completed']])
measure_plan2complete   = measure_total_bp - measure_beyond_deadline

measures.append(measure_total_bp)
measures.append(measure_sm_count)
measures.append(measure_scm_count)
measures.append(measure_beyond_deadline)
measures.append(measure_steps_completed)
measures.append(measure_plan2complete)

# for index, row in df_g.iterrows():
#     if row.completed == 0 and isinstance(row.Steps_Completed, pd.Timestamp):
#         print ('moshe')

df_g['anomaly'] = df_g.apply(
    lambda x: 'anomaly' if x.completed == 0 and isinstance(x.Steps_Completed, pd.Timestamp) else 'ok', axis=1)

df_anomaly = df_g[df_g['anomaly'] == 'anomaly']

df_step = df_1[df_1['PercentC'] == 0]
df_2 = pd.DataFrame()
for p in df_step['process_id'].unique():
    df_z = df_step[df_step['process_id'] == p]
    df_z = df_z.loc[df_z.index.min(), :]
    df_2 = df_2.append(df_z)

df_2 = df_2[['RN', 'process_id', 'Name_Ordered', 'Finish_Date']].reset_index()
df_2 = df_2[['RN', 'process_id', 'Name_Ordered', 'Finish_Date']]
df_2 = df_2.rename(
    columns={
        'process_id'   : 'process',
        'Name_Ordered': 'current status',
        'Finish_Date'  : 'due date'
    }
)

writer = pd.ExcelWriter(fo, engine='xlsxwriter')
workbook = writer.book
df_g.to_excel(writer, sheet_name='Sheet1')
df_x.to_excel(writer, sheet_name='Sheet2')
df_1.to_excel(writer, sheet_name='Sheet3')
writer.save()

writer = pd.ExcelWriter(cs, engine='xlsxwriter')
df_2.to_excel(writer, sheet_name='current_status')
writer.save()

step_ordered  = config['step_ordered']

moshe = '4 - אישור מפקדים'
df_2 = df_g[df_g['Name_Ordered'] == moshe]
df_2.set_index('process_id', inplace=True)
df_2_dict = df_2.to_dict()
df_waiting['x'] = df_waiting.process_id.map(df_2_dict.get('Finish_Date'))
df_letters['x'] = pd.to_datetime('2021/01/31')

fig = px.timeline(
    df_g,
    x_start='Start_Date',
    x_end='Finish_Date',
    y='process_id',
    color='Name_Ordered',
    color_discrete_sequence=px.colors.qualitative.Pastel,
    hover_name='process_id',
    hover_data={
        'process_id'  : False,
        'Name_Ordered': True,
        'Start_Date'  : True
    },
    title='Gantt Report: ' + today.strftime('%d/%m/%Y'),
)
for ser in fig['data']:
    ser['hovertemplate'] = \
        '<b>%{hovertext}</b><br><br>step  = %{customdata[1]}<br>start = %{customdata[2]|%Y-%m-%d}<br>' \
        'finish= %{x|%Y-%m-%d}<extra></extra>'

fig.add_scatter(
    y=df_x['process_id'],
    x=df_x['Steps_Completed'],
    name='completed',
    mode='markers',
    marker={'size': 10, 'symbol': 'square', 'color': 'black'},
    hovertemplate='<b>%{text}</b>',
    text=['completed: {}'.format(df_x.Steps_Completed[i].strftime('%Y-%m-%d')) for i in range(len(df_x))],
    showlegend=False
)

fig.add_scatter(
    y=df_waiting['process_id'],
    x=df_waiting['x'],
    name='רשימת המתנה',
    mode='markers',
    marker={'size': 15, 'symbol': 'star', 'color': 'red', 'opacity': 0.95},
    hovertemplate='<b>%{text}</b>',
    text=['waiting for: <br><br>{}'.format(df_waiting.waiting_list[i]) for i in range(len(df_waiting))],
    showlegend=True
)
'''
fig.add_scatter(
    y=df_letters['process_id'],
    x=df_letters['x'],
    name='מוריה',
    mode='markers',
    marker={'size': 15, 'symbol': 'circle', 'color': 'blue', 'opacity': 0.95},
    showlegend=True,
    hoverinfo='none'
)
'''

fig.update_layout(
    hoverlabel=dict(
        bgcolor='white',
        font_size=16,
        font_family='Courier New, monospace'
    )
)

point_shape = config['rect_attr']
point_shape.pop('xref')
point_shape.pop('yref')
progress_line = config['progress_line']
history_line  = config['history_line']
for i, x in enumerate(df_x['process_id']):
    xz = [df_x.Start_Date[i], df_x.Steps_Completed[i]]
    yz = [df_x.process_id[i], df_x.process_id[i]]
    # progress_line['x0'] = df_x.Start_Date[i]
    # progress_line['x1'] = df_x.Steps_Completed[i]
    # progress_line['y0'] = df_x.process_id[i]
    # progress_line['y1'] = df_x.process_id[i]

    fig.add_trace(go.Scatter(
        x=xz,
        y=yz,
        mode='lines',
        # name='progress',
        line=dict(
            color='firebrick',
            width=4
        ),
        showlegend=False
    ))
    # fig.add_shape(progress_line)
    history_line['x0'] = df_x.Start_Date[i]
    try:
        history_line['x1'] = df_gantt_history.Finish_Date[i]
    except KeyError:
        history_line['x1'] = df_x.Start_Date[i]

    history_line['y0'] = i + 0.3
    history_line['y1'] = i + 0.3
    fig.add_shape(history_line, name='baseline')

'''
fig.add_scatter(
    y=df_gantt_history['process_id'],
    x=df_gantt_history['Steps_Completed'],
    name='תאריך סיום בדיווח הקודם',
    mode='markers',
    marker={'size': 10, 'symbol': 'square', 'color': '#192ecf', 'opacity': 1.0},
    hovertemplate='<b>%{text}</b>',
    text=['last completion date: <br><br>{}'.format(df_gantt_history.Steps_Completed[i].strftime('%Y-%m-%d')) for i in range(len(df_gantt_history))],
    showlegend=True
)

fig.add_scatter(
    y=df_gantt_history['process_id'],
    x=df_gantt_history['Finish_Date'],
    name='baseline',
    mode='markers',
    marker={'size': 10, 'symbol': 'cross-open-dot', 'color': '#192ecf', 'opacity': 1.0},
    hovertemplate='<b>%{text}</b>',
    text=['baseline finish date: <br><br>{}'.format(df_gantt_history.Finish_Date[i].strftime('%Y-%m-%d')) for i in range(len(df_gantt_history))],
    showlegend=True
)
'''

threshold_line                 = config['threshold_line']
threshold_line['x0']           = today
threshold_line['x1']           = today
threshold_line['line']['dash'] = 'dot'
fig.add_shape(threshold_line)

g_today         = config['gantt_today']
g_today['x']    = today
g_today['text'] = 'Today'
fig.add_annotation(g_today)

fig.update_xaxes(title_font_family='Arial')
fig.update_yaxes(autorange="reversed")
fig.update_layout(showlegend=True, xaxis_range=[x_start,
                               pd.to_datetime('2021/01/31', format='%Y/%m/%d')])

fn = os.path.join(config['config'].get('web'), 'bp_plan.html')
pyo.plot(fig, filename=fn)

quit(0)
