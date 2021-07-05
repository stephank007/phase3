import os
import codecs
import pandas as pd
import warnings as warning
import yaml
import mongo_services as ms

pd.options.mode.chained_assignment = 'raise'  # pd.options.mode.chained_assignment = None
warning.filterwarnings('ignore')
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')

p3 = os.path.join(path, config['config'].get('name'))
f2 = os.path.join(path, config['config'].get('requirements'))
fr = os.path.join(path, config['config'].get('pmo'))
fs = os.path.join(path, config['config'].get('skeleton'))
mo = os.path.join(path, config['config'].get('moria'))
bi = os.path.join(path, config['config'].get('p3BI'))
f1 = os.path.join(path, config['config'].get('out'))


df_dm      = pd.read_excel(p3, sheet_name='domain_ref')
df_pr      = pd.read_excel(p3, sheet_name='process_ref')
df_mm      = pd.read_excel(p3, sheet_name='MoM')
df_in      = pd.read_excel(p3, sheet_name='interfaces')
df_sc      = pd.read_excel(p3, sheet_name='scm_tasks')

df_rq      = pd.read_excel(f2, sheet_name='דרישות')
df_ts      = pd.read_excel(f2, sheet_name='משימות')

df_rl      = pd.read_excel(fr, sheet_name='Lines')
df_hd      = pd.read_excel(fr, sheet_name='header')

df_bp      = pd.read_excel(fs, sheet_name='skeleton')

df_sm      = pd.read_excel(mo, sheet_name='SM')
df_cm      = pd.read_excel(mo, sheet_name='SCM')
df_sm_text = pd.read_excel(mo, sheet_name='SM_text')
df_sc_text = pd.read_excel(mo, sheet_name='SCM_text')

df_bi      = pd.read_excel(bi, sheet_name='Task_Table1')

df_fP      = pd.read_excel(f1, sheet_name='Task_Table1')
df_ot      = pd.read_excel(f1, sheet_name='Sheet1')

df_dict = {
    'skeleton'    : df_bp,
    'domain_ref'  : df_dm,
    'process_ref' : df_pr,
    'MoM'         : df_mm,
    'requirements': df_rq,        # from shared file with dev team
    'tasks'       : df_ts,
    'interfaces'  : df_in,
    'scm_tasks'   : df_sc,
    'Lines'       : df_rl,
    'header'      : df_hd,
    'BI'          : df_bi,
    'preBI'       : df_ot,        # out file Sheet1 - all data
    'dfP'         : df_fP,        # out file selected columns and renamed for project input sheet
    'SM'          : df_sm,
    'SCM'         : df_cm,
    'SM_text'     : df_sm_text,
    'SCM_text'    : df_sc_text
}

df_bp['due_date']        = df_bp['due_date'].astype(str)
df_bp['Deadline']        = df_bp['Deadline'].astype(str)

df_pr['due_date']        = df_pr['due_date'].astype(str)
df_pr['Deadline']        = df_pr['Deadline'].astype(str)

df_mm['due_date']        = df_mm['due_date'].astype(str)

df_rq['due_date']        = df_rq['due_date'].astype(str)
df_ts['due_date']        = df_ts['due_date'].astype(str)

df_in['due_date']        = df_in['due_date'].astype(str)
df_in['Deadline']        = df_in['Deadline'].astype(str)

df_sc['due_date']        = df_sc['due_date'].astype(str)

df_hd['Deadline']        = df_hd['Deadline'].astype(str)

df_bi['Start_Date']      = df_bi['Start_Date'].astype(str)
df_bi['Finish_Date']     = df_bi['Finish_Date'].astype(str)


df_ot['due_date']        = df_ot['due_date'].astype(str)
df_ot['Finish']          = df_ot['Finish'].astype(str)
df_ot['Start']           = df_ot['Start'].astype(str)
df_ot['Constraint Date'] = df_ot['Constraint Date'].astype(str)


df_fP['Finish']          = df_fP['Finish'].astype(str)
df_fP['Start']           = df_fP['Start'].astype(str)
df_fP['Constraint Date'] = df_fP['Constraint Date'].astype(str)


db = ms.mongo_connect()
for name in df_dict:
    collection = ms.get_collection(db, name)
    df         = df_dict.get(name)
    print('\n', name, df.shape)
    ms.delete_history(collection, ms.get_date())
    ms.add_records(df, collection)

    ms.print_history_index(collection)
