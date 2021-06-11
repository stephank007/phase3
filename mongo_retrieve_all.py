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

path = config['history'].get('path')
pmo  = os.path.join(path, config['history'].get('pmo'))
fn   = os.path.join(path, config['history'].get('name'))
mo   = os.path.join(path, config['history'].get('moria'))

# df_list = [df_bp     , df_pr        , df_mm,  df_rq        , df_in       , df_sc      , df_rl  , df_ms, df_hd]
df_name = ['skeleton', 'process_ref', 'MoM', 'requirements', 'interfaces', 'scm_tasks', 'Lines', 'MS' , 'header', 'SM', 'SCM']

db = ms.mongo_connect()
dt = config['history'].get('date')

writer = pd.ExcelWriter(pmo, engine='xlsxwriter')
for sheet in ['Lines', 'MS' , 'header']:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_all(collection)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()

print('\nnext file')

writer = pd.ExcelWriter(fn, engine='xlsxwriter')
for sheet in ['BI', 'skeleton', 'process_ref', 'MoM', 'requirements', 'interfaces', 'scm_tasks']:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_all(collection)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()

print('\nnext file')

writer = pd.ExcelWriter(mo, engine='xlsxwriter')
for sheet in ['SM' , 'SCM']:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_all(collection)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()
