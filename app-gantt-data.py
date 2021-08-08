import os
import codecs
import time
import yaml
import pandas as pd
import numpy as np
import workday as wd
from dateutil.relativedelta import relativedelta

start_s = time.time()
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

publish = config['execution_params'].get('publish')
pc = '% Complete'

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
f6 = os.path.join(path, config['config'].get('requirements'))
fn = os.path.join(path, config['config'].get('out'))
fb = os.path.join(path, config['config'].get('p3BI'))
fo = os.path.join(path, config['config'].get('gantt'))
cs = os.path.join(path, config['config'].get('c_status'))
dc = os.path.join(path, config['config'].get('dev-cmp'))
bl = os.path.join(baseline, config['config'].get('gantt'))

df_p3   = pd.read_excel(fn, sheet_name='Sheet1')           # p3-out file
df_bi   = pd.read_excel(fb, sheet_name='Task_Table1')      # BI file
df_pr   = pd.read_excel(f1, sheet_name='process_ref')      # phase-3 - process_ref sheet
df_task = pd.read_excel(f3, sheet_name='task_mapping')
df_pdr  = pd.read_excel(f4, sheet_name='dl')
m4n_mm  = pd.read_excel(f4, sheet_name='m4n-mm')
df_sl   = pd.read_excel(f5, sheet_name='Sheet1')
df_dev  = pd.read_excel(f6, sheet_name='דרישות')

df_grand_totals = pd.DataFrame()

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

sl_map = df_sl.copy()
sl_map = sl_map[sl_map[pc] == 1]
sl_map_xRN_list = sl_map['RN'].to_list()
sl_map.set_index('RN', inplace=True)
sl_map = sl_map.to_dict()

""" build baseline reference map for all INT requirements """
bl_gantt_int_map = pd.read_excel(bl, sheet_name='raw-gantt-data')
bl_gantt_int_map = bl_gantt_int_map[bl_gantt_int_map['rule'] == 'INT']
bl_gantt_int_map = bl_gantt_int_map[bl_gantt_int_map['RN'].str.endswith('.02.03')]
bl_gantt_int_map = bl_gantt_int_map.groupby(['domain_id', 'process_id']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
    }
)

bl_gantt_int_map.reset_index(inplace=True)
bl_gantt_int_map['key'] = bl_gantt_int_map.apply(lambda x: '-'.join([x.domain_id, x.process_id]), axis=1)
bl_gantt_int_map.set_index('key', inplace=True)
bl_gantt_int_map = bl_gantt_int_map.to_dict()

""" prepare shafir data """
# ms_list = ['SRR', 'PDR', 'CDR', 'INFRA', 'INTEG', 'אספקה נכנסת', 'ניהול חצר', 'אספקה יוצאת', 'ניהול מלאי', 'M4N-PDR', 'M4N-CDR']
ms_list = ['SRR', 'PDR', 'CDR', 'INFRA', 'INTEG', 'M4N-PDR', 'M4N-CDR']
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

df_pr = df_pr[['process_id', 'BP', 'process']]  # process reference
df_pr['process'] = df_pr.process.apply(lambda x: f'{x:0>5}')
df_pr.set_index('process', inplace=True)
pr_map = df_pr.to_dict()

df_milestones = pd.DataFrame()
for index, row in df_pdr.iterrows():
    p_list = row.p_id.split(',')
    p_list = [f'{p.strip():0>5}' for p in p_list]
    for p in p_list:
        df_milestones = df_milestones.append({
            'p_id'        : p,
            'process_id'  : df_pr.loc[p, 'process_id'],
            'wbs'         : row.WBS,
            'name'        : '{} -- {}'.format(row.topic, row.task),
            'finish'      : row.finish
        }, ignore_index=True)
df_milestones.set_index('process_id', inplace=True)
df_milestones_map = df_milestones.to_dict()

df_pdr = pd.concat([df_pdr, m4n_mm], ignore_index=True)
df_pdr.set_index('WBS', inplace=True)
pdr_map = df_pdr.to_dict()

df_deliverables['bl-date']      = df_deliverables.source.map(pdr_map.get('bl_date'))
df_deliverables['finish-month'] = pd.to_datetime(df_deliverables['Finish_Date']).dt.strftime('%Y-%m')
df_deliverables['bl-month']     = pd.to_datetime(df_deliverables['bl-date']).dt.strftime('%Y-%m')
df_deliverables['y']            = 30
df_deliverables                 = df_deliverables[df_deliverables['bl-month'].notnull()]
""" end m4n preparation """

df_task['Name'] = df_task['Name'].str.strip()
df_task.set_index('Name', inplace=True)
task_map = df_task.to_dict()

""" df_p3 is the out file"""
df_p3 = df_p3[['RN', 'domain_id', 'process_id']]
df_p3.set_index('RN', inplace=True)
p3_map = df_p3.to_dict()

df_pr = df_pr[['process_id', 'BP']]  # process reference
df_pr.set_index('process_id', inplace=True)
pr_map = df_pr.to_dict()

""" df_bi augmentation and fixing """
df_bi['process']     = df_bi['process'].apply(lambda x: f'{x:0>5}')
df_bi['Start_Date']  = pd.to_datetime(df_bi['Start_Date'],  format='%d %B %Y %H:%M')
df_bi['Finish_Date'] = pd.to_datetime(df_bi['Finish_Date'], format='%d %B %Y %H:%M')

df_bi = df_bi.rename(columns={'%C': 'pc'})

df_bi['Name']    = df_bi['Name'].str.strip()
df_bi['m_task']  = df_bi.Name.map(task_map.get('Step'))
df_bi['publish'] = df_bi.process_id.map(pr_map.get('BP'))

df_bi['month'] = df_bi['Finish_Date'].dt.strftime('%Y-%m')
df_bi['total'] = 1

for xrn in sl_map_xRN_list:
    bi_row = df_bi[df_bi['xRN'] == xrn]
    p_rn   = bi_row.parent.unique()[0]
    p_name = df_bi[df_bi['RN'] == p_rn]['Name'].values[0]
    bi_rn  = bi_row.parent.unique() + '.02.03'
    bi_row = df_bi[df_bi['RN'].isin(bi_rn)]
    df_bi.loc[bi_row.index, 'Start_Date']  = df_bi.xRN.map(sl_map.get('Start'))
    df_bi.loc[bi_row.index, 'Finish_Date'] = df_bi.xRN.map(sl_map.get('Finish'))
    df_bi.loc[bi_row.index, 'p_name']      = p_name
""" END df_bi augmentation and fixing """

""" preparing process list start finish percent complete(pc) """
process_list = df_bi[df_bi['publish'] == 'publish']['process'].unique()
c_map = df_bi.copy()
c_map = c_map.groupby(['process_id']).agg({
    'Start_Date' : 'min'
})
c_map.reset_index(inplace=True)

task_ET_map = df_bi.copy()
task_ET_map = task_ET_map[( task_ET_map['m_task'].notnull() ) & ( task_ET_map['publish'] == 'publish' )]
task_ET_map = task_ET_map.groupby(['domain_id', 'process_id', 'm_task']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
    }
)
task_ET_map.reset_index(inplace=True)

for index, row in task_ET_map.iterrows():
    df_x = task_ET_map[task_ET_map.process_id == row.process_id]
    df_x = df_x.copy()
    i_min = df_x.index.min()
    i_max = df_x.index.max()
    df_x['s_date'] = df_x['Finish_Date'].shift(1)
    s_date = c_map[c_map['process_id'] == row.process_id]['Start_Date'].values[0]
    df_x.loc[i_min, 's_date'] = s_date
    task_ET_map.loc[i_min:i_max, 'Start_Date'] = df_x['s_date']

task_ET_map['ET'] = ( task_ET_map['Finish_Date'] - task_ET_map['Start_Date'] ).dt.days
task_ET_map = task_ET_map[task_ET_map['m_task'] == 'Dev']
task_ET_map.set_index('process_id', inplace=True)
task_ET_map = task_ET_map.to_dict()
''' '''
bp_list = [x + '.BP' for x in process_list]
df_bp = df_bi[df_bi['RN'].isin(bp_list)]
df_bp = df_bp.copy()
df_bp['Duration'] = df_bp['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_bp['c_days']   = df_bp['Duration'] * df_bp['pc']
df_bp['c_date']   = df_bp.apply(
    lambda x: wd.workday(
        x['Start_Date'],
        days=x['c_days'],
        holidays=holidays,
        weekends=weekends
    ) if x.pc < 1 else x.Finish_Date, axis=1
)
bp_map = df_bp.copy()
bp_map.set_index('process_id', inplace=True)
''' '''
df_logmar = df_bi[df_bi['RN'].str.endswith('.02.03')]
df_logmar = df_logmar[df_logmar['source'] == 'logmar']
df_logmar['duration'] = df_logmar['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_logmar['c_days']   = df_logmar['duration'] * df_logmar['pc']

df_logmar_completed   = df_logmar.groupby(['domain_id', 'process_id']).agg({
    'c_days'     : 'sum',
    'duration'   : 'sum',
    'Start_Date' : 'min'
})
df_logmar_completed.reset_index(inplace=True)

df_logmar_completed['logmar_pc']  = df_logmar_completed['c_days'] / df_logmar_completed['duration']   # calculate %Completed per process_id
df_logmar_completed['ET']         = df_logmar_completed.process_id.map(task_ET_map.get('ET'))         # bring ElapseTime for map
df_logmar_completed['c_days']     = df_logmar_completed['ET'] * df_logmar_completed['logmar_pc']
df_logmar_completed['c_days']     = df_logmar_completed['c_days'].apply(lambda x: x if x > 0 else 0)
df_logmar_completed['Start_Date'] = df_logmar_completed.process_id.map(bp_map.get('Finish_Date'))     # override logmar Start_Date
df_logmar_completed['c_date']     = pd.to_datetime(df_logmar_completed['Start_Date'] + pd.to_timedelta(df_logmar_completed['c_days'], unit='d'))

df_logmar_completed.set_index('process_id', inplace=True)
df_logmar_completed.rename(columns={'Start_Date' : 'logmar_sd'}, inplace=True)
logmar_map = df_logmar_completed.to_dict()

bp_list = [x + '.BP' for x in process_list]
df_bp = df_bi[df_bi['RN'].isin(bp_list)]
df_bp = df_bp.copy()
df_bp['Duration'] = df_bp['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_bp['c_days']   = df_bp['Duration'] * df_bp['pc']
df_bp['c_date']   = df_bp.apply(
    lambda x: wd.workday(
        x['Start_Date'],
        days=x['c_days'],
        holidays=holidays,
        weekends=weekends
    ) if x.pc < 1 else x.Finish_Date, axis=1
)
df_completed = df_bp.copy()
df_completed['logmar_c_date']   = df_completed.process_id.map(logmar_map.get('c_date'))
df_completed['logmar_duration'] = df_completed.process_id.map(logmar_map.get('duration'))
df_completed['logmar_c_days']   = df_completed.process_id.map(logmar_map.get('c_days'))
df_completed['logmar_sd']       = df_completed.process_id.map(logmar_map.get('logmar_sd'))
df_completed['logmar_pc']       = df_completed.process_id.map(logmar_map.get('logmar_pc'))
df_completed['logmar_pc']       = df_completed['logmar_pc'].apply(lambda x: x if x > 0 else 0)

df_completed['max_date']  = df_completed[['c_date', 'logmar_c_date']].max(axis=1)
df_completed['bp_c_date'] = df_completed['c_date']
df_completed['c_date']    = df_completed['max_date']
df_completed = df_completed[[
    'RN', 'domain_id', 'process_id',  'Start_Date', 'Finish_Date', 'c_date', 'bp_c_date', 'max_date', 'c_days',
    'logmar_duration', 'logmar_c_days', 'logmar_pc', 'logmar_sd', 'logmar_c_date'
]]
""" finish of marking completed date (c_date) preparations """

parent_map = df_bi[df_bi['row_type'] == 'parent']
parent_map = parent_map[['parent', 'Name', 'xRN', 'rule']]
parent_map.set_index('parent', inplace=True)
parent_map = parent_map.to_dict()

""" marimekko chart preparation: dealing with the development counting closed versus open per development month"""
df_bx = df_bi[( df_bi['m_task'].notnull() ) & ( df_bi['source'] == 'logmar' )]
df_bx = df_bx[( df_bx['RN'].str.endswith('.02.03') ) ]  # filter development tasks
df_bx = df_bx.copy()
df_bx['p_name']   = df_bx.parent.map(parent_map.get('Name'))
df_bx['xRN']      = df_bx.parent.map(parent_map.get('xRN'))
df_bx['rule']     = df_bx.parent.map(parent_map.get('rule'))
df_bx['Duration'] = df_bx['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_bx['c_days']   = df_bx['Duration'] * df_bx['pc']
df_bx['completed']= df_bx['pc'].apply(lambda x: 1 if x == 1 else 0)
df_bx = df_bx[['xRN', 'parent', 'RN', 'month', 'Finish_Date', 'rule', 'pc', 'domain_id', 'process_id', 'completed', 'p_name']]

""" df_t1 table is for gantt-data drawing blocks of start and stop of the development lifecycle """
df_bi = df_bi[( df_bi['m_task'].notnull() ) & ( df_bi['publish'] == 'publish' )]
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
df_t1['key']          = df_t1.apply(lambda x: '-'.join([x.domain_id, x.process_id, x.m_task]), axis=1)

df_t1['bl-finish']    = df_t1.key.map(bl_gantt_data_map.get('Finish_Date'))          # finish from gantt-data sheet
df_t1.sort_values(['process_id'], ascending=False, inplace=True)
""" END of df_t1"""

df_completed['bl-finish'] = df_completed.key.map(bl_completed_map.get('Finish_Date'))
df_completed['m4n-ms']    = df_completed.process_id.map(df_milestones_map.get('finish'))
df_completed['m4n-nm']    = df_completed.process_id.map(df_milestones_map.get('name'))

""" prepare INT sheet """
df_int = df_bi[df_bi['RN'].str.endswith('.02.03')]
df_int = df_int[df_int['rule'] == 'INT']
df_int['Duration'] = df_int['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_int['c_days'] = df_int['Duration'] * df_int['pc']
df_int = df_int.groupby(['domain_id', 'process_id']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
        'c_days'     : 'sum'
    }
)

df_int.reset_index(inplace=True)
df_int['key']       = df_int.apply(lambda x: '-'.join([x.domain_id, x.process_id]), axis=1)
df_int['bl-finish'] = df_int.key.map(bl_gantt_int_map.get('Finish_Date'))
df_int['duration']  = df_int.apply(lambda x: len(pd.date_range(x.Start_Date, x.Finish_Date, freq=custombday)), axis=1)
df_int['c_days']    = ( df_int['c_days'] / df_int['duration'] ) * df_int['duration']
df_int['c_date']   = df_int.apply(
    lambda x: wd.workday(
        x['Start_Date'],
        days=x['c_days'],
        holidays=holidays,
        weekends=weekends
    ), axis=1
)
df_int['m_task'] = 'Dev'
df_int.sort_values(['process_id'], ascending=False, inplace=True)
""" END prepare INT sheet """

""" EARNED VALUE """
f1 = os.path.join(path,     config['config'].get('name'))
f5 = os.path.join(path,     config['config'].get('shortlist'))
f7 = os.path.join(baseline, config['config'].get('shortlist'))  # baseline shortlist
bi = os.path.join(path,     config['config'].get('p3BI'))

bi_map       = pd.read_excel(bi, sheet_name='Task_Table1')   # BI File
pr_map       = pd.read_excel(f1, sheet_name='process_ref')   # phase-3 - process_ref sheet
df_shortlist = pd.read_excel(f5, sheet_name='Sheet1')
bl_shortlist = pd.read_excel(f7, sheet_name='Sheet1')

pr_map            = pr_map[['process', 'domain_id', 'process_id', 'BP', 'weight']]
pr_map['process'] = pr_map['process'].apply(lambda x: f'{x:0>5}')
pr_map.set_index('process', inplace=True)
pr_map = pr_map.to_dict()

bi_map.set_index('xRN', inplace=True)
bi_map = bi_map.to_dict()

bl_shortlist               = bl_shortlist.rename(columns={'RN': 'xRN'})
bl_shortlist['RN']         = bl_shortlist.xRN.map(bi_map.get('RN'))
bl_shortlist['process']    = bl_shortlist.xRN.map(bi_map.get('process'))
bl_shortlist['process']    = bl_shortlist['process'].apply(lambda x: f'{x:0>5}')
bl_shortlist['process_id'] = bl_shortlist.xRN.map(bi_map.get('process_id'))
bl_shortlist['weight']     = bl_shortlist.process.map(pr_map.get('weight'))
bl_shortlist['Duration']   = bl_shortlist['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
bl_shortlist['month']      = pd.to_datetime(bl_shortlist['Finish']).dt.strftime('%Y-%m')
bl_shortlist['budget']     = bl_shortlist['Duration'] * bl_shortlist['weight']

df_budget = bl_shortlist.groupby('month').agg({
    'budget' : 'sum'
})
df_budget.reset_index(inplace=True)
df_budget['budget_cumsum'] = df_budget['budget'].cumsum().astype(int)

budget_map = df_budget.copy()
budget_map.set_index('month', inplace=True)
budget_map = budget_map.to_dict()

""" PIVOT number of requirements as per process id and rule """
df_shortlist               = df_shortlist.rename(columns={'RN': 'xRN'})
df_shortlist['process_id'] = df_shortlist.xRN.map(bi_map.get('process_id'))
df_shortlist['RN']         = df_shortlist.xRN.map(bi_map.get('RN'))
df_shortlist['process']    = df_shortlist.xRN.map(bi_map.get('process'))
df_shortlist['rule']       = df_shortlist.xRN.map(bi_map.get('rule'))
df_process = df_shortlist.groupby([ 'process_id', 'rule' ]).agg({
    'RN': 'count'
})
df_process.reset_index(inplace=True)
df_rn_totals = pd.pivot_table(
    df_process,
    values='RN',
    index=['process_id'],
    columns=['rule'], aggfunc=np.sum
)
df_rn_totals.reset_index(inplace=True)
""" END PIVOT"""

df_shortlist['process']    = df_shortlist['process'].apply(lambda x: f'{x:0>5}')
df_shortlist['weight']     = df_shortlist.process.map(pr_map.get('weight'))
df_shortlist['Duration']   = df_shortlist['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_shortlist['month']      = pd.to_datetime(df_shortlist['Finish']).dt.strftime('%Y-%m')
df_shortlist['completed']  = df_shortlist[pc].apply(lambda x: 1 if x == 1 else 0)
df_shortlist['value']      = df_shortlist['Duration'] * df_shortlist['weight']
df_shortlist['completed-value'] = df_shortlist['completed'] * df_shortlist['value']

df_pc = df_shortlist.groupby('month').agg({
    'completed-value' : 'sum',
    'RN'              : 'size'
})
df_pc.reset_index(inplace=True)

df_pc['m_date']         = pd.to_datetime(df_pc['month'], format='%Y-%m').dt.strftime('%Y%m%d').astype(int)
df_pc['c-value-cumsum'] = df_pc['completed-value'].cumsum()
df_pc['budget']         = df_pc.month.map(budget_map.get('budget'))
df_pc['ytd-budget']     = df_pc.apply(lambda x: None if x.m_date >  int(today.strftime('%Y%m%d')) else x.budget, axis=1)
df_pc['ytd-value']      = df_pc.apply(lambda x: None if x.m_date >  int(today.strftime('%Y%m%d')) else x['c-value-cumsum'], axis=1)

pc_map = df_pc[['month', 'ytd-budget', 'ytd-value']]
pc_map.set_index('month', inplace=True)
pc_map = pc_map.to_dict()

df_budget['m_date'] = pd.to_datetime(df_budget['month'], format='%Y-%m').dt.strftime('%Y%m%d').astype(int)
df_budget['value']  = df_budget.month.map(pc_map.get('ytd-value'))
df_budget['ev']     = df_budget['value']
df_budget['spi']    = df_budget['ev'] / df_budget['budget_cumsum']

SPI = df_budget[df_budget['month'] == today.strftime('%Y-%m')]['spi'].values[0]
print(SPI)
""" END EARNED VALUE """

""" Grand Totals  from requirements shortlist"""
df_sl['xRN'] = df_sl.index

df_sl.rename(columns=config['realization_columns_map'], inplace=True)
df_sl['rule'] = df_sl['s_rule'].apply(lambda x: config['s_rule_map'].get(x))
df_grand_totals = df_grand_totals.append({
    'BP'           : len(process_list),
    'requirements' : len(df_dev),
    'shortlist'    : len(df_sl),
    'sl_close'     : len(df_sl[df_sl[pc] == 1.0]),
    'sap'          : len(df_sl[df_sl['rule'] == 'SAP']),
    'int'          : len(df_sl[df_sl['rule'] == 'INT']),
    'SPI'          : SPI
}, ignore_index=True)
""" END Grand Totals """

writer   = pd.ExcelWriter(fo, engine='xlsxwriter')
workbook = writer.book
df_bi.to_excel(writer,           sheet_name='raw-gantt-data')
df_t1.to_excel(writer,           sheet_name='gantt-data')
df_bx.to_excel(writer,           sheet_name='raw-dev')
df_completed.to_excel(writer,    sheet_name='completed')
df_contractor.to_excel(writer,   sheet_name='shafir')
df_deliverables.to_excel(writer, sheet_name='deliverables')
df_int.to_excel(writer,          sheet_name='int')
baseline_data.to_excel(writer,   sheet_name='bl-month')
df_budget.to_excel(writer,       sheet_name='EVM')
df_rn_totals.to_excel(writer,    sheet_name='RN_Totals')
df_grand_totals.to_excel(writer, sheet_name='g-totals')

writer.save()
