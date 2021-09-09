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

path     = config['config'].get('path')
baseline = config['config'].get('baseline')

f1 = os.path.join(path,     config['config'].get('name'))
f3 = os.path.join(path,     config['config'].get('pmo'))
f6 = os.path.join(path,     config['config'].get('requirements'))
fn = os.path.join(path,     config['config'].get('out'))
fb = os.path.join(path,     config['config'].get('p3BI'))
fo = os.path.join(path,     config['config'].get('gantt'))
bl = os.path.join(baseline, config['config'].get('gantt'))
f7 = os.path.join(baseline, config['config'].get('shortlist'))      # baseline shortlist
f4 = os.path.join(config['config'].get('pdr-meeting'))

df_bi            = pd.read_excel(fb, sheet_name='Task_Table1')      # BI file
df_pr            = pd.read_excel(f1, sheet_name='process_ref')      # phase-3 - process_ref sheet
df_task          = pd.read_excel(f3, sheet_name='task_mapping')
df_pdr           = pd.read_excel(f4, sheet_name='dl')
df_dev           = pd.read_excel(f6, sheet_name='דרישות')
bl_shortlist     = pd.read_excel(f7, sheet_name='Sheet1')
bl_gantt_int_map = pd.read_excel(bl, sheet_name='raw-gantt-data')
bl_gantt_data_map= pd.read_excel(bl, sheet_name='gantt-data')
# bl_completed_map = pd.read_excel(bl, sheet_name='completed')

"""  Preparation steps """
writer   = pd.ExcelWriter(fo, engine='xlsxwriter')
workbook = writer.book

pc       = '% Complete'
pc_level = config['execution_params'].get('pc_level')
holidays = []
for d in config['bdays'].get('holidays'):
    holidays.append(pd.to_datetime(d))

holidays   = pd.to_datetime(config['holidays'])
weekmask   = config['bdays'].get('bdays')
custombday = pd.offsets.CustomBusinessDay(weekmask=weekmask, holidays=holidays)

(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
weekends = (FRI, SAT)

today = pd.to_datetime('today')
if today.day < 16:
    today_mm = ( today - pd.DateOffset(months=1) ).strftime('%Y-%m')
else:
    today_mm = today.strftime('%Y-%m')

print(today_mm)

deadline = pd.to_datetime('2020/10/31', format='%Y/%m/%d')
x_start  = pd.to_datetime('2020/08/01', format='%Y/%m/%d')

lc_list = ['BP', 'SAP', 'INT', 'Test', 'Prod']

df_gt         = pd.DataFrame()
baseline_data = pd.DataFrame()

bl_month      = pd.to_datetime(config['dates'].get('bl_date'), format='%d/%m/%Y').strftime('%Y-%m')
baseline_data = baseline_data.append({'key': bl_month}, ignore_index=True)
baseline_data['pc_level'] = pc_level

bl_shortlist['Duration'] = bl_shortlist['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)
df_bi['Duration']        = df_bi['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)

df_bi                = df_bi.rename(columns={pc: 'pc'})
df_bi['process']     = df_bi['process'].apply(lambda x: f'{x:0>5}')
df_bi['Start_Date']  = pd.to_datetime(df_bi['Start_Date'],  format='%d %B %Y %H:%M')
df_bi['Finish_Date'] = pd.to_datetime(df_bi['Finish_Date'], format='%d %B %Y %H:%M')
""" END of some preparation steps """

""" ********** All Mapping Creation """
# bl_completed_map.set_index('key', inplace=True)
# bl_completed_map = bl_completed_map.to_dict()

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

df_sl            = df_bi[df_bi['source'] == 'logmar']
df_sl = df_sl.copy()
df_sl['process'] = df_sl['process'].apply(lambda x: f'{x:0>5}')

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
df_bi['m_task']  = df_bi.Name.map(task_map.get('Step'))  # map major lifecycle to all tasks
df_bi['publish'] = df_bi.process.map(pr_map.get('BP'))

process_list = list(df_bi[df_bi['publish'] == 'publish']['process'].unique())
sl_plist     = list(df_sl['process'].unique())
p_list       = process_list

process_list = sorted(list(set(process_list + sl_plist)))
sl_plist = set(sl_plist)
p_list = set(p_list)
no_bp = [process for process in p_list if process not in p_list]
no_bp = sorted(sl_plist.difference(p_list))
for p in no_bp:
    print(p)
cycle_map = df_bi[( df_bi['m_task'].notnull() ) & ( df_bi['process'].isin(process_list) )]
cycle_map = cycle_map.copy()
cycle_map['c_days'] = cycle_map['Duration'] * cycle_map['pc']
cycle_map = cycle_map.groupby(['domain_id', 'process_id', 'm_task']).agg(  # find min/max dates for each Step
    {
        'Duration'   : 'sum',
        'c_days'     : 'sum',
        'Start_Date' : 'min',
        'Finish_Date': 'max',
    }
).reset_index()
cycle_map['pc'] = cycle_map['c_days'] / cycle_map['Duration']
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
df_sl['weight']     = df_sl.process.map(pr_map.get('weight'))
df_sl['month']      = pd.to_datetime(df_sl['Finish_Date']).dt.strftime('%Y-%m')
df_sl['new_budget'] = df_sl['weight'] * df_sl['Duration']
df_sl['old_budget'] = df_sl.xRN.map(bl_map.get('budget'))
df_sl['old_budget'] = df_sl.apply(lambda x: x.new_budget if np.isnan(x.old_budget) else x.old_budget, axis=1)
df_sl['c_days']     = df_sl['Duration'] * df_sl['pc']
df_sl['Name']       = df_sl.xRN.map(parent_map.get('Name'))

df_sl['completed']  = df_sl['pc'].apply(lambda x: 1 if x >= pc_level else 0)
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

dd = int(today.replace(day=1).strftime('%Y%m%d'))

df_budget['m_date']        = pd.to_datetime(df_budget['month'], format='%Y-%m').dt.strftime('%Y%m%d').astype(int)
df_budget['value-cumsum']  = df_budget['value'].cumsum()
df_budget['budget']        = df_budget['old_budget']
df_budget['budget-cumsum'] = df_budget['budget'].cumsum()
df_budget['n-bdgt-cumsum'] = df_budget['new_budget'].cumsum()
df_budget['ytd-value']     = df_budget.apply(lambda x: None if x.m_date >= dd else x['value-cumsum'], axis=1)
df_budget['ytd-budget']    = df_budget.apply(lambda x: None if x.m_date >= dd else x['budget-cumsum'], axis=1)
df_budget['spi']           = df_budget['ytd-value'] / df_budget['budget-cumsum']
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
    ) if x.pc < pc_level else x.Finish_Date, axis=1
)
''' END BP '''
""" calculate start and finish for the SAP lane """
df_sap = cycle_map[cycle_map['m_task'] == 'SAP']

for index, row in df_sap.iterrows():
    bp_index = df_bp[df_bp['process_id'] == row.process_id].index
    df_bp.loc[bp_index, 'sap_sd'] = row.Start_Date
    df_bp.loc[bp_index, 'sap_fd'] = row.Finish_Date
df_sap.set_index('process_id', inplace=True)

df_bp['sap_duration'] = df_bp.process_id.map(df_sap.get('Duration'))
df_bp['sap_c_days']   = df_bp.process_id.map(df_sap.get('c_days'))
df_bp['sap_pc']       = df_bp.process_id.map(df_sap.get('pc'))
df_bp['bp_fd']        = df_bp['c_date']
df_bp['ET']           = (df_bp['sap_fd'] - df_bp['sap_sd']).dt.days
df_bp['c_days']       = df_bp['ET'] * df_bp['sap_pc']
df_bp['c_date']       = pd.to_datetime(df_bp['sap_sd'] + pd.to_timedelta(df_bp['c_days'], unit='d'))
df_bp['c_date']       = df_bp.apply(lambda x: x.bp_fd if x.sap_pc == 0 else x.c_date, axis=1)
df_bp['sap_pc'] = df_bp['sap_pc'].fillna(0)

""" marimekko chart preparation: dealing with the development counting closed versus open per development month"""
df_mekko = df_sl.copy()
df_mekko['p_name']   = df_mekko.parent.map(parent_map.get('Name'))
df_mekko['rule']     = df_mekko.parent.map(parent_map.get('rule'))
df_mekko['completed']= df_mekko['pc'].apply(lambda x: 1 if x >= pc_level else 0)
df_mekko = df_mekko[[
    'xRN', 'parent', 'RN', 'month', 'Finish_Date', 'rule', 'Duration', 'pc',
    'domain_id', 'weight', 'value', 'process_id', 'process', 'completed', 'p_name'
]]
""" END of marimekko chart: dealing with the development counting closed versus open per development month"""

""" df_t1 table is for gantt-data drawing blocks of start and stop of the development lifecycle """
df_t1 = cycle_map.copy()
"""" trying to exclude BP lines 
df_t1['process'] = df_t1['process_id'].str.split(':').apply(lambda x: x[0])
df_t1 = df_t1[~( df_t1['process'].isin(no_bp) ) & ~( df_t1['m_task'] == 'BP' )]
"""

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
""" fix TEST start date as per the max finish date between Dev and Int"""
t1_int = df_t1[df_t1['m_task'] == 'INT']
t1_dev = df_t1[df_t1['m_task'] == 'SAP']
t1_tst = df_t1[df_t1['m_task'] == 'Test']
for index, row in t1_tst.iterrows():
    try:
        int_fd = t1_int[t1_int['process_id'] == row.process_id]['Finish_Date'].values[0]
        dev_fd = t1_dev[t1_dev['process_id'] == row.process_id]['Finish_Date'].values[0]
    except Exception as ex:
        continue
    fd = max(int_fd, dev_fd)
    df_t1.loc[index, 'Start_Date'] = fd
""" fix Int start date since we wish to have it appear as overlap within Dev """
for index, row in t1_int.iterrows():
    c_map_sd = cycle_map[( cycle_map['m_task'] == row.m_task ) & ( cycle_map['process_id'] == row.process_id )]['Start_Date'].values[0]
    df_t1.loc[index, 'Start_Date'] = c_map_sd
for index, row in t1_dev.iterrows():
    bp_index = df_bp[df_bp['process_id'] == row.process_id].index
    df_bp.loc[bp_index, 'dev_fd'] = row.Finish_Date
df_bp['dev_fd'] = df_bp.apply(lambda x: x.Finish_Date if x.sap_pc == 0 else x.dev_fd, axis=1)

dff = cycle_map[cycle_map['m_task'] == 'SAP']
dff = dff.groupby(by=['process_id']).agg({'Start_Date': 'min'}).reset_index()
for index, row in dff.iterrows():
    bp_index = df_bp[df_bp['process_id'] == row.process_id].index
    t1_index = df_t1[( df_t1['process_id'] == row.process_id ) & ( df_t1['m_task'] == 'SAP' )].index
    df_bp.loc[bp_index, 'dev_sd'] = row.Start_Date
    df_t1.loc[t1_index, 'Start_Date'] = row.Start_Date
df_bp['dev_sd'] = df_bp.apply(lambda x: x.Finish_Date if x.sap_pc == 0 else x.dev_sd, axis=1)
df_bp = df_bp[['domain_id', 'process_id', 'sap_pc', 'Start_Date', 'Finish_Date', 'sap_sd', 'sap_fd',
               'bp_fd', 'c_date', 'dev_sd', 'dev_fd', 'c_days', 'ET', 'Duration']]

df_t1['m_task'] = pd.Categorical(df_t1['m_task'], categories=lc_list, ordered=False)     # keep category order as appears on the list
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
df_int['c_date']    = df_int.apply(
    lambda x: wd.workday(
        x['Start_Date'],
        days=x['c_days'],
        holidays=holidays,
        weekends=weekends
    ), axis=1
)
df_int['m_task'] = 'INT'
df_int.sort_values(['process_id'], ascending=False, inplace=True)
""" END prepare INT sheet """
prev_mm   = ( today - pd.DateOffset(months=1) ).strftime('%Y-%m')
SPI       = df_budget[df_budget['month'] == prev_mm]['spi'].values[0]
PC        = df_sap['c_days'].sum() / df_sap['Duration'].sum()
logmar_ET = ( logmar_finish_date - logmar_start_date ).days
target    = ( today - logmar_start_date ).days / logmar_ET
actual    = int( PC * logmar_ET ) / logmar_ET
print(SPI, PC, logmar_ET, target, actual)
""" Grand Totals  from requirements shortlist"""
df_gt = df_gt.append({
    'BP'           : len(process_list),
    'requirements' : len(df_dev),
    'shortlist'    : len(df_sl),
    'sl_close'     : len(df_sl[df_sl['pc'] >= pc_level]),
    'sap'          : len(df_sl[df_sl['rule'] == 'SAP']),
    'int'          : len(df_sl[df_sl['rule'] == 'INT']),
    'SPI'          : SPI,
    'PC'           : PC,
    'target'       : target,
    'actual'       : actual,
    'YTD_tasks'    : df_mekko[(df_mekko['month'] <= today_mm)].shape[0],
    'YTD_Closed'   : df_mekko[(df_mekko['month'] <= today_mm) & ( df_mekko['pc'] >= pc_level )].shape[0],
    'YTD_Open'     : df_mekko[(df_mekko['month'] <= today_mm) & ( df_mekko['pc'] <  pc_level)].shape[0],
    'zero_priced'  : df_sl[df_sl['Duration'] == 0].shape[0],
    'still_open'   : df_sl[( df_sl['month'] <= today_mm ) & ( df_sl['pc'] < pc_level )].shape[0],
    'open_lt_69'   : df_sl[( df_sl['month'] <= today_mm ) & ( df_sl['pc'] < 0.69 )].shape[0]
}, ignore_index=True)

df_bi    = df_bi.rename(columns={'pc': pc})
df_sl    = df_sl.rename(columns={'pc': pc})
df_mekko = df_mekko.rename(columns={'pc': pc})

not_started = df_sl.groupby(by=['process_id']).sum([pc]).reset_index()
not_started = not_started[not_started[pc] == 0]

df_completed = df_bp.copy()
df_completed = df_completed[['process_id', 'sap_sd', 'sap_fd', 'c_date', 'sap_pc']]
columns = ('process_id', 'process_date')
data = []
for index, row in df_completed.iterrows():
    row_1 = dict(zip(columns, [row.process_id, row.sap_sd]))
    data.append(row_1)
    if row.sap_pc > 0:
        row_2 = dict(zip(columns, [row.process_id, row.c_date]))
    else:
        row_2 = dict(zip(columns, [row.process_id, row.sap_sd]))
    data.append(row_2)

process_dates = pd.DataFrame(data)

df_bi.to_excel(writer,         sheet_name='raw-gantt-data')
df_t1.to_excel(writer,         sheet_name='gantt-data')
df_sl.to_excel(writer,         sheet_name='raw-dev')
df_bp.to_excel(writer,         sheet_name='completed')
df_int.to_excel(writer,        sheet_name='int')
baseline_data.to_excel(writer, sheet_name='bl-month')
df_budget.to_excel(writer,     sheet_name='EVM')
df_rn_totals.to_excel(writer,  sheet_name='RN_Totals')
df_gt.to_excel(writer,         sheet_name='g-totals')
not_started.to_excel(writer,   sheet_name='not_started')
process_dates.to_excel(writer, sheet_name='process_dates')
bl_shortlist.to_excel(writer,  sheet_name='bl_shortlist')

writer.save()
