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
today_mm = today.strftime('%Y-%m')
deadline = pd.to_datetime('2020/10/31', format='%Y/%m/%d')
x_start  = pd.to_datetime('2020/08/01', format='%Y/%m/%d')

path     = config['config'].get('path')
baseline = config['config'].get('baseline')

f1 = os.path.join(path,     config['config'].get('name'))
f2 = os.path.join(path,     config['config'].get('name'))
f3 = os.path.join(path,     config['config'].get('pmo'))
f6 = os.path.join(path,     config['config'].get('requirements'))
f7 = os.path.join(baseline, config['config'].get('shortlist'))  # baseline shortlist
fn = os.path.join(path,     config['config'].get('out'))
fb = os.path.join(path,     config['config'].get('p3BI'))
fo = os.path.join(path,     config['config'].get('gantt'))
cs = os.path.join(path,     config['config'].get('c_status'))
dc = os.path.join(path,     config['config'].get('dev-cmp'))
bl = os.path.join(baseline, config['config'].get('gantt'))
f4 = os.path.join(config['config'].get('pdr-meeting'))

df_bi            = pd.read_excel(fb, sheet_name='Task_Table1')      # BI file
df_pr            = pd.read_excel(f1, sheet_name='process_ref')      # phase-3 - process_ref sheet
df_task          = pd.read_excel(f3, sheet_name='task_mapping')
df_pdr           = pd.read_excel(f4, sheet_name='dl')
df_dev           = pd.read_excel(f6, sheet_name='דרישות')
bl_shortlist     = pd.read_excel(f7, sheet_name='Sheet1')
bl_gantt_int_map = pd.read_excel(bl, sheet_name='raw-gantt-data')
bl_completed_map = pd.read_excel(bl, sheet_name='completed')
bl_gantt_data_map= pd.read_excel(bl, sheet_name='gantt-data')

""" Some preparation steps """
writer   = pd.ExcelWriter(fo, engine='xlsxwriter')
workbook = writer.book

baseline_data = pd.DataFrame()
bl_month = pd.to_datetime(config['dates'].get('bl_date'), format='%d/%m/%Y').strftime('%Y-%m')
baseline_data = baseline_data.append({
    'key': bl_month
}, ignore_index=True)

lc_list = ['BP', 'Dev', 'Test', 'Prod']

df_gt = pd.DataFrame()
bl_shortlist['Duration']    = bl_shortlist['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)

df_bi['Duration']    = df_bi['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_bi                = df_bi.rename(columns={pc: 'pc'})
df_bi['process']     = df_bi['process'].apply(lambda x: f'{x:0>5}')
df_bi['Start_Date']  = pd.to_datetime(df_bi['Start_Date'],  format='%d %B %Y %H:%M')
df_bi['Finish_Date'] = pd.to_datetime(df_bi['Finish_Date'], format='%d %B %Y %H:%M')
""" END of some preparation steps """

""" ********** All Mapping Creation """
bl_completed_map.set_index('key', inplace=True)
bl_completed_map = bl_completed_map.to_dict()

bl_gantt_data_map.set_index('process_id', inplace=True)
bl_gantt_data_map = bl_gantt_data_map.to_dict()

parent_map = df_bi[df_bi['row_type'] == 'parent']
parent_map = parent_map[['parent', 'Name', 'xRN', 'rule', 'process', 'process_id', 'domain_id']]
parent_map = parent_map.rename(columns={'parent': 'RN'})
parent_map.set_index('xRN', inplace=True)
parent_map = parent_map.to_dict()

df_pr = df_pr[['process_id', 'BP', 'process', 'weight']]  # process reference
df_pr['process'] = df_pr.process.apply(lambda x: f'{x:0>5}')
df_pr.set_index('process', inplace=True)
pr_map = df_pr.to_dict()

df_sl = df_bi[df_bi['source'] == 'logmar']
df_sl = df_sl.copy()

df_task['Name'] = df_task['Name'].str.strip()
df_task.set_index('Name', inplace=True)
task_map = df_task.to_dict()

bl_shortlist               = bl_shortlist.rename(columns={'RN': 'xRN'})  # change name to xRN
bl_shortlist['RN']         = bl_shortlist.xRN.map(parent_map.get('RN'))
bl_shortlist['process']    = bl_shortlist.xRN.map(parent_map.get('process'))
bl_shortlist['process']    = bl_shortlist['process'].apply(lambda x: f'{x:0>5}')
bl_shortlist['domain_id']  = bl_shortlist.xRN.map(parent_map.get('domain_id'))
bl_shortlist['process_id'] = bl_shortlist.xRN.map(parent_map.get('process_id'))
bl_shortlist['weight']     = bl_shortlist.process.map(pr_map.get('weight'))
bl_shortlist['month']      = pd.to_datetime(bl_shortlist['Finish']).dt.strftime('%Y-%m')
bl_shortlist['budget']     = bl_shortlist['weight'] * bl_shortlist['Duration']
bl_shortlist['rule']       = bl_shortlist.xRN.map(parent_map.get('rule'))

bl_map = bl_shortlist.copy()
bl_map.set_index('xRN', inplace=True)
bl_map = bl_map.to_dict()

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

""" df_bi augmentation with references """
df_bi['Name']    = df_bi['Name'].str.strip()
df_bi['m_task']  = df_bi.Name.map(task_map.get('Step'))
df_bi['publish'] = df_bi.process.map(pr_map.get('BP'))
process_list = df_bi[df_bi['publish'] == 'publish']['process'].unique()

df_published = df_bi[( df_bi['m_task'].notnull() ) & ( df_bi['publish'] == 'publish' )]
df_published = df_published.copy()

cycle_map = df_published.groupby(['domain_id', 'process_id', 'm_task']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
    }
)
cycle_map.reset_index(inplace=True)
""" END of df_bi augmentation with references """
""" **********************  END of all maps ************************ """

""" Shortlist PIVOT"""
df_process = df_sl.groupby([ 'process_id', 'rule' ]).agg({
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
""" END shortlist PIVOT"""
df_sl['process']    = df_sl['process'].apply(lambda x: f'{x:0>5}')
df_sl['weight']     = df_sl.process.map(pr_map.get('weight'))
df_sl['month']      = pd.to_datetime(df_sl['Finish_Date']).dt.strftime('%Y-%m')
df_sl['new_budget'] = df_sl['weight'] * df_sl['Duration']
df_sl['old_budget'] = df_sl.xRN.map(bl_map.get('budget'))
df_sl['old_budget'] = df_sl.apply(lambda x: x.new_budget if np.isnan(x.old_budget) else x.old_budget, axis=1)
df_sl['c_days']     = df_sl['Duration'] * df_sl['pc']

df_sl['completed']  = df_sl['pc'].apply(lambda x: 1 if x == 1.0 else 0)
df_sl['value']      = df_sl['old_budget'] * df_sl['completed']
df_sl = df_sl[[
    'RN', 'xRN', 'process', 'domain_id', 'process_id', 'Start_Date', 'Finish_Date', 'pc', 'parent',
    'rule', 'Duration', 'weight', 'value', 'month', 'new_budget', 'old_budget', 'c_days', 'completed', 'Name'
]]

logmar_start_date  = df_sl['Start_Date'].min()
logmar_finish_date = df_sl['Finish_Date'].max()
""" END of df_sl is the new shortlist, now coming from df_bi """

""" EARNED VALUE """
df_budget = df_sl.groupby('month').agg({
    'old_budget' : 'sum',
    'new_budget' : 'sum',
    'value'      : 'sum',
    'RN'         : 'size'
})
df_budget.reset_index(inplace=True)

df_budget['m_date']        = pd.to_datetime(df_budget['month'], format='%Y-%m').dt.strftime('%Y%m%d').astype(int)
df_budget['value-cumsum']  = df_budget['value'].cumsum()
df_budget['budget']        = df_budget['old_budget']
df_budget['budget-cumsum'] = df_budget['budget'].cumsum()
df_budget['ytd-budget']    = df_budget.apply(lambda x: None if x.m_date > int(today.strftime('%Y%m%d')) else x['budget-cumsum'], axis=1)
df_budget['ytd-value']     = df_budget.apply(lambda x: None if x.m_date > int(today.strftime('%Y%m%d')) else x['value-cumsum'], axis=1)
df_budget['spi']           = df_budget['value-cumsum'] / df_budget['budget-cumsum']
""" END Earned Value """

""" logmar completed task calculation """
''' BP Map '''
bp_finish_map = cycle_map[cycle_map['m_task'] == 'BP']
bp_finish_map.set_index('process_id', inplace=True)

bp_list = [x + '.BP' for x in process_list]
df_bp = df_bi[df_bi['RN'].isin(bp_list)]
df_bp = df_bp.copy()
df_bp = df_bp[['RN', 'domain_id', 'process_id', 'Start_Date', 'Finish_Date', 'Duration', 'pc', 'publish']]
df_bp['Finish_Date'] = df_bp.process_id.map(bp_finish_map.get('Finish_Date'))

df_bp['c_days'] = df_bp['Duration'] * df_bp['pc']
df_bp['c_date'] = df_bp.apply(
    lambda x: wd.workday(
        x['Start_Date'],
        days=x['c_days'],
        holidays=holidays,
        weekends=weekends
    ) if x.pc < 1 else x.Finish_Date, axis=1
)
''' END BP '''
df_logmar   = df_sl.groupby(['domain_id', 'process_id']).agg({
    'c_days'     : 'sum',
    'Duration'   : 'sum',
    'Finish_Date': 'max'
})
df_logmar.reset_index(inplace=True)
df_logmar.set_index('process_id', inplace=True)

df_bp['logmar_duration'] = df_bp.process_id.map(df_logmar.get('Duration'))
df_bp['logmar_c_days']   = df_bp.process_id.map(df_logmar.get('c_days'))
df_bp['logmar_fd']       = df_bp.process_id.map(df_logmar.get('Finish_Date'))
df_bp['logmar_pc']       = ( df_bp['logmar_c_days'] / df_bp['logmar_duration'] ).fillna(0)
df_bp['bp_c_date']       = df_bp['c_date']
df_bp['ET']              = (df_bp['logmar_fd'] - df_bp['Finish_Date']).dt.days
df_bp['c_days']          = df_bp['ET'] * df_bp['logmar_pc']
df_bp['c_date']          = pd.to_datetime(df_bp['Finish_Date'] + pd.to_timedelta(df_bp['c_days'], unit='d'))
df_bp['c_date']          = df_bp.apply(lambda x: x.bp_c_date if x.logmar_pc == 0 else x.c_date, axis=1)
df_bp = df_bp[['domain_id', 'process_id', 'Start_Date', 'Finish_Date', 'logmar_fd', 'bp_c_date', 'c_date', 'c_days', 'ET', 'logmar_pc', 'Duration']]
""" marimekko chart preparation: dealing with the development counting closed versus open per development month"""
df_mekko = df_sl.copy()
df_mekko['p_name']   = df_mekko.parent.map(parent_map.get('Name'))
df_mekko['rule']     = df_mekko.parent.map(parent_map.get('rule'))
df_mekko['completed']= df_mekko['pc'].apply(lambda x: 1 if x == 1 else 0)
df_mekko = df_mekko[[
    'xRN', 'parent', 'RN', 'month', 'Finish_Date', 'rule', 'Duration', 'pc',
    'domain_id', 'weight', 'value', 'process_id', 'process', 'completed', 'p_name'
]]
""" END of marimekko chart: dealing with the development counting closed versus open per development month"""

""" df_t1 table is for gantt-data drawing blocks of start and stop of the development lifecycle """
df_t1 = df_published.groupby(['domain_id', 'process_id', 'm_task']).agg(
    {
        'Start_Date' : 'min',
        'Finish_Date': 'max',
    }
)
df_t1.reset_index(inplace=True)

df_t1['m_task'] = df_t1['m_task'].astype('category')
df_t1['m_task'] = pd.Categorical(df_t1['m_task'], categories=lc_list, ordered=False)
df_t1.sort_values(['domain_id', 'process_id', 'm_task'], inplace=True)
df_t1.reset_index(inplace=True)

for index, row in df_t1.iterrows():
    df_x = df_t1[df_t1.process_id == row.process_id]
    try:
        logmar_c_date = df_bp[df_bp.process_id == row.process_id]['Start_Date'].values[0]
    except IndexError as ex:
        logmar_c_date = df_bp.loc[row['process_id'], 'Start_Date']
        continue
    df_x = df_x.copy()
    df_x['s_date'] = df_x['Finish_Date'].shift(1)
    df_t1.loc[df_x.index.min():df_x.index.max(), 'Start_Date'] = df_x['s_date']
    df_x.loc[df_x.index.min(), 's_date'] = logmar_c_date
    df_t1.loc[df_x.index.min():df_x.index.max(), 'Start_Date'] = df_x['s_date']
df_t1['bl-finish'] = df_t1.process_id.map(bl_gantt_data_map.get('Finish_Date'))          # finish from gantt-data sheet

df_t1['m_task'] = pd.Categorical(df_t1['m_task'], categories=lc_list, ordered=False)
df_t1.sort_values(['domain_id', 'process_id', 'm_task'], inplace=True, ascending=False)
""" END of df_t1"""
""" prepare INT sheet """
df_int = df_sl[df_sl['rule'] == 'INT']
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
SPI       = df_budget[df_budget['month'] == today.strftime('%Y-%m')]['spi'].values[0]
PC        = df_logmar['c_days'].sum() / df_logmar['Duration'].sum()
logmar_ET = ( logmar_finish_date - logmar_start_date ).days
target    = ( today - logmar_start_date ).days / logmar_ET
actual    = int( PC * logmar_ET ) / logmar_ET
print(SPI, PC, logmar_ET, target, actual)
""" Grand Totals  from requirements shortlist"""
df_gt = df_gt.append({
    'BP'           : len(process_list),
    'requirements' : len(df_dev),
    'shortlist'    : len(df_sl),
    'sl_close'     : len(df_sl[df_sl['pc'] == 1.0]),
    'sap'          : len(df_sl[df_sl['rule'] == 'SAP']),
    'int'          : len(df_sl[df_sl['rule'] == 'INT']),
    'SPI'          : SPI,
    'PC'           : PC,
    'target'       : target,
    'actual'       : actual,
    'YTD_tasks'    : df_mekko[(df_mekko['month'] <= today_mm)].shape[0],
    'YTD_Closed'   : df_mekko[(df_mekko['month'] <= today_mm) & ( df_mekko['pc'] == 1 )].shape[0],
    'YTD_Open'     : df_mekko[(df_mekko['month'] <= today_mm) & (df_mekko['pc'] <= 1)].shape[0],
}, ignore_index=True)

df_bi    = df_bi.rename(columns={'pc': pc})
df_sl    = df_sl.rename(columns={'pc': pc})
df_mekko = df_mekko.rename(columns={'pc': pc})

df_bi.to_excel(writer,         sheet_name='raw-gantt-data')
df_t1.to_excel(writer,         sheet_name='gantt-data')
df_sl.to_excel(writer,         sheet_name='raw-dev')
df_bp.to_excel(writer,         sheet_name='completed')
df_int.to_excel(writer,        sheet_name='int')
baseline_data.to_excel(writer, sheet_name='bl-month')
df_budget.to_excel(writer,     sheet_name='EVM')
df_rn_totals.to_excel(writer,  sheet_name='RN_Totals')
df_gt.to_excel(writer,         sheet_name='g-totals')

writer.save()
