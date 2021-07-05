import os
import codecs
import time
import yaml
import pandas as pd
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

path     = config['config'].get('path')
baseline = config['config'].get('baseline')

f1 = os.path.join(path,  config['config'].get('name'))
f2 = os.path.join(path,  config['config'].get('name'))
f3 = os.path.join(path,  config['config'].get('pmo'))
f4 = os.path.join(config['config'].get('pdr-meeting'))
f5 = os.path.join(path,  config['config'].get('shortlist'))
fn = os.path.join(path,  config['config'].get('out'))
fb = os.path.join(path,  config['config'].get('p3BI'))
fo = os.path.join(path,  config['config'].get('gantt'))
cs = os.path.join(path,  config['config'].get('c_status'))
bl = os.path.join(baseline, config['config'].get('gantt'))
dc = os.path.join(path, config['config'].get('dev-cmp'))

df_p3   = pd.read_excel(fn, sheet_name='Sheet1')           # p3-out file
df_bi   = pd.read_excel(fb, sheet_name='Task_Table1')      # BI file
df_pr   = pd.read_excel(f1, sheet_name='process_ref')      # phase-3 - process_ref sheet
df_task = pd.read_excel(f3, sheet_name='task_mapping')
df_pdr  = pd.read_excel(f4, sheet_name='dl')
m4n_mm  = pd.read_excel(f4, sheet_name='m4n-mm')
df_sl   = pd.read_excel(f5, sheet_name='Sheet1')

""" baseline mapping """
bl_dev_map = pd.read_excel(bl, sheet_name='dev')
baseline_data = pd.DataFrame(columns=['key'], data=[[bl_dev_map['Finish_Date'].max().strftime('%Y-%m')]])
bl_dev_map.set_index('key', inplace=True)
bl_dev_map = bl_dev_map.to_dict()

bl_completed_map = pd.read_excel(bl, sheet_name='completed')
bl_completed_map.set_index('key', inplace=True)
bl_completed_map = bl_completed_map.to_dict()

bl_gantt_data_map = pd.read_excel(bl, sheet_name='gantt-data')
bl_gantt_data_map.set_index('key', inplace=True)
bl_gantt_data_map = bl_gantt_data_map.to_dict()

""" prepare shafir data """
ms_list = ['SRR', 'PDR', 'CDR', 'INFRA', 'INTEG', 'אספקה נכנסת', 'ניהול חצר', 'אספקה יוצאת', 'ניהול מלאי', 'M4N-PDR', 'M4N-CDR']
df_contractor = df_bi[df_bi['C_MS'].isin(ms_list)]
df_contractor = df_contractor[['C_MS', 'Finish_Date']]
df_contractor['month'] = pd.to_datetime(df_contractor['Finish_Date']).dt.strftime('%Y-%m')
df_contractor['y'] = 30

df_d = df_contractor.copy()
df_d.sort_values('month', inplace=True)
df_d.reset_index(inplace=True)
df_d['next'] = df_d['month'].shift(-1)
# df_d.loc[df_d.index.max(), 'next'] = df_d.loc[df_d.index.max(), 'month']

i = 1
for index, row in df_d.iterrows():
    if row['month'] == row['next']:
        df_d.loc[index, 'y'] = 30 + (10 * i)
        i += 1
    else:
        i = 1
    df_contractor.loc[row['index'], 'y'] = df_d.loc[index, 'y']
""" end shafir data """

""" prepare m4n deliverables data """
df_deliverables = df_bi[df_bi['row_type'].isin(['m4n-dl', 'm4n-ms'])]
df_deliverables = df_deliverables.copy()
df_deliverables['Finish_Date'] = pd.to_datetime(df_deliverables['Finish_Date'])
df_deliverables = df_deliverables[['RN', 'Name', 'source', 'Finish_Date', 'row_type']]

df_pdr = pd.concat([df_pdr, m4n_mm], ignore_index=True)
df_pdr.set_index('WBS', inplace=True)
pdr_map = df_pdr.to_dict()

df_deliverables['bl-date'] = df_deliverables.source.map(pdr_map.get('bl_date'))

df_deliverables['finish-month'] = pd.to_datetime(df_deliverables['Finish_Date']).dt.strftime('%Y-%m')
df_deliverables['bl-month']     = pd.to_datetime(df_deliverables['bl-date']).dt.strftime('%Y-%m')
df_deliverables['y'] = 30
df_deliverables = df_deliverables[df_deliverables['bl-month'].notnull()]

""" end m4n preparation """

""" arrange BI file """
df_task['Name'] = df_task['Name'].str.strip()
df_task.set_index('Name', inplace=True)
task_map = df_task.to_dict()

""" df_p3 is the out file"""
df_p3 = df_p3[['RN', 'domain_id', 'process_id']]
df_p3.set_index('RN', inplace=True)
p3_map = df_p3.to_dict()

df_pr = df_pr[['process_id', 'BP']]
df_pr.set_index('process_id', inplace=True)
pr_map = df_pr.to_dict()

df_bi['Start_Date']  = pd.to_datetime(df_bi['Start_Date'])
df_bi['Finish_Date'] = pd.to_datetime(df_bi['Finish_Date'])

df_bi['domain_id']  = df_bi.RN.map(p3_map.get('domain_id'))
df_bi['process_id'] = df_bi.RN.map(p3_map.get('process_id'))
df_bi = df_bi.rename(columns={'%C': 'pc'})

df_bi['Name']    = df_bi['Name'].str.strip()
df_bi['m_task']  = df_bi.Name.map(task_map.get('Step'))
df_bi['publish'] = df_bi.process_id.map(pr_map.get('BP'))

df_bi['month'] = df_bi['Finish_Date'].dt.strftime('%Y-%m')
df_bi['total'] = 1

""" preparing process list start finish percent complete(pc) """
process_list = df_bi[df_bi['publish'] == 'publish']['process_id'].unique()
process_list = [p.split(':')[0] for p in process_list]

df_completed = df_bi[df_bi['RN'].isin(process_list)]
df_completed = df_completed.copy()

df_completed['Duration'] = df_completed['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_completed['c_days']   = df_completed['Duration'] * df_completed['pc']
df_completed['c_date']   = df_completed.apply(
    lambda x: wd.workday(
        x['Start_Date'],
        days=x['c_days'],
        holidays=holidays, weekends=weekends
    ), axis=1
)
df_completed = df_completed[['RN', 'domain_id', 'process_id',  'Start_Date', 'Finish_Date', 'c_date', 'pc', 'c_days']]
""" finish of process list preparations """

parent_map = df_bi[df_bi['row_type'] == 'parent']
parent_map = parent_map[['parent', 'Name', 'xRN', 'rule']]
parent_map.set_index('parent', inplace=True)
parent_map = parent_map.to_dict()

""" marimekko chart preparation: dealing with the development counting closed versus open per development month"""
df_bi = df_bi[( df_bi['m_task'].notnull() ) & ( df_bi['publish'] == 'publish' )]
df_bx = df_bi[( df_bi['RN'].str.contains('.02.03') ) & ( df_bi['m_task'] == 'Dev' )]  # filter development tasks
df_bx = df_bx.copy()
df_bx['p_name']   = df_bx.parent.map(parent_map.get('Name'))
df_bx['xRN']      = df_bx.parent.map(parent_map.get('xRN'))
df_bx['rule']     = df_bx.parent.map(parent_map.get('rule'))
""" comparing development task to the shortlist (df_sl) """
df_sl['Duration'] = df_sl['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_sl['Start']  = pd.to_datetime(df_sl['Start'], format='%d/%m/%Y')
df_sl['Finish'] = pd.to_datetime(df_sl['Finish'], format='%d/%m/%Y')
df_sl.set_index('RN', inplace=True)
sl_map = df_sl.to_dict()
df_bx['sl-start']    = df_bx.xRN.map(sl_map.get('Start'))
df_bx['sl-finish']   = df_bx.xRN.map(sl_map.get('Finish'))
df_bx['sl-duration'] = df_bx.xRN.map(sl_map.get('Duration'))

df_bx['Duration'] = df_bx.Duration.apply(lambda x: x.split(' ')[0]).astype(float)
df_monthly_cut = df_bx.groupby(['domain_id', 'process_id', 'rule', 'month']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
        'total'      : 'sum',
        'pc'         : 'sum',
        'Duration'   : 'sum'
    }
)
df_monthly_cut.reset_index(inplace=True)
df_monthly_cut['closed'] = df_monthly_cut['pc']
df_monthly_cut['open']   = df_monthly_cut['total'] - df_monthly_cut['closed']

""" df_t1 table is for gantt-data drawing blocks of start and stop of the development lifecycle """
df_t1 = df_bi.groupby(['domain_id', 'process_id', 'm_task']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
    }
)
lc_list = ['BP', 'Dev', 'Test', 'Prod']

df_t1.reset_index(inplace=True)
df_t1['m_task'] = df_t1['m_task'].astype('category')
df_t1['m_task'] = pd.Categorical(df_t1['m_task'], categories=lc_list, ordered=False)
df_t1.sort_values(['domain_id', 'process_id', 'm_task'], inplace=True)
df_t1.reset_index(inplace=True)

for index, row in df_t1.iterrows():
    df_x = df_t1[df_t1.process_id == row.process_id]
    df_c = df_completed[df_completed.process_id == row.process_id]
    df_x = df_x.copy()
    df_x['s_date'] = df_x['Finish_Date'].shift(1)
    df_x.loc[df_x.index.min(), 's_date'] = df_c.loc[df_c.index.min(), 'Start_Date']
    df_t1.loc[df_x.index.min():df_x.index.max(), 'Start_Date'] = df_x['s_date']

df_bi = df_bi[config['df_bi_columns']]

df_completed['key']   = df_completed['process_id']
df_monthly_cut['key'] = df_monthly_cut.apply(lambda x: '-'.join([x.domain_id, x.process_id, x.rule, x.month]), axis=1)
df_t1['key']          = df_t1.apply(lambda x: '-'.join([x.domain_id, x.process_id, x.m_task]), axis=1)

df_t1['bl-finish']        = df_t1.key.map(bl_gantt_data_map.get('Finish_Date'))          # finish from gantt-data sheet
df_completed['bl-finish'] = df_completed.key.map(bl_completed_map.get('Finish_Date'))
df_t1.sort_values(['process_id'], ascending=False, inplace=True)


writer   = pd.ExcelWriter(fo, engine='xlsxwriter')
workbook = writer.book

df_bi.to_excel(writer,           sheet_name='raw-gantt-data')
df_t1.to_excel(writer,           sheet_name='gantt-data')
df_bx.to_excel(writer,           sheet_name='raw-dev')
df_monthly_cut.to_excel(writer,  sheet_name='dev')
df_completed.to_excel(writer,    sheet_name='completed')
df_contractor.to_excel(writer,   sheet_name='shafir')
df_deliverables.to_excel(writer, sheet_name='deliverables')
baseline_data.to_excel(writer,   sheet_name='bl-month')

writer.save()
df_bx = df_bx[['RN', 'xRN', 'Start_Date', 'Finish_Date', 'sl-start', 'sl-finish', 'Duration', 'sl-duration', 'domain_id', 'process_id', 'rule', 'preds']]
df_bx['diff_in_finish'] = pd.to_timedelta(df_bx['Finish_Date'] - df_bx['sl-finish'], unit='d')
df_bx['diff_in_start']  = pd.to_timedelta(df_bx['Start_Date']  - df_bx['sl-start'], unit='d')
writer   = pd.ExcelWriter(dc, engine='xlsxwriter')
workbook = writer.book
df_bx.to_excel(writer, sheet_name='dev-compare')
writer.save()
