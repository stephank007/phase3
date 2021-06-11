import os
import codecs
import time
import yaml
import pandas as pd

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

# baseline = True if config['execution_params'].get('baseline') else False
# history  = config['execution_params'].get('history')
path = config['config'].get('path')

f1 = os.path.join(path,  config['config'].get('name'))
f2 = os.path.join(path,  config['config'].get('name'))
f3 = os.path.join(path,  config['config'].get('rule'))
fn = os.path.join(path,  config['config'].get('out'))
fb = os.path.join(path,  config['config'].get('p3BI'))
fo = os.path.join(path,  config['config'].get('gantt'))
cs = os.path.join(path,  config['config'].get('c_status'))
# hs = os.path.join(path,  config['history'].get('gantt_bl'))

writer   = pd.ExcelWriter(fo, engine='xlsxwriter')
workbook = writer.book

start_d = pd.to_datetime('02/10/2020', format='%d/%m/%Y').strftime('%m/%d/%Y')

df_p3 = pd.read_excel(fn, sheet_name='Sheet1')             # p3-out file
df_bi = pd.read_excel(fb, sheet_name='Task_Table1')        # BI file
df_pr = pd.read_excel(f1, sheet_name='process_ref')        # phase-3 - process_ref sheet
# df_gantt_history = pd.read_excel(hs, sheet_name='Sheet2')  # gantt history

df_task_map = pd.read_excel(f3, sheet_name='task_mapping')
df_task_map.set_index('Name', inplace=True)
task_map = df_task_map.to_dict()

df_p3 = df_p3[['RN', 'domain_id', 'process_id']]
df_p3.set_index('RN', inplace=True)
p3_map = df_p3.to_dict()

df_bi['domain_id']  = df_bi.RN.map(p3_map.get('domain_id'))
df_bi['process_id'] = df_bi.RN.map(p3_map.get('process_id'))
# df_bi = df_bi[['RN', 'Name', 'Start_Date', 'Finish_Date', 'PercentC', 'Duration', 'domain_id', 'process_id', 'rule']]
df_bi = df_bi.rename(columns={'PercentC': 'pc'})
df_bi['Start_Date']  = pd.to_datetime(df_bi['Start_Date'], format='%d %B %Y %H:%M')
df_bi['Finish_Date'] = pd.to_datetime(df_bi['Finish_Date'], format='%d %B %Y %H:%M')

df_pr = df_pr[['process_id', 'BP']]
df_pr.set_index('process_id', inplace=True)
pr_map = df_pr.to_dict()

df_bi['m_task']       = df_bi.Name.map(task_map.get('Step'))
df_bi['publish']      = df_bi.process_id.map(pr_map.get('BP'))

df_bi = df_bi[( df_bi['m_task'].notnull() ) & ( ~df_bi['RN'].str.contains('I-') ) & ( df_bi['publish'] == 'publish' )]
df_t1 = df_bi.groupby(['domain_id', 'process_id', 'm_task']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
        'pc'         : 'max'
    }
)
df_t1.sort_values(['process_id', 'Start_Date'], inplace=True, ascending=False)
df_t1.reset_index(inplace=True)

df_bi.to_excel(writer, sheet_name='raw-gantt-data')
df_t1.to_excel(writer, sheet_name='gantt-data')

writer.save()
