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
fr   = os.path.join(path, config['history'].get('requirements'))
mo   = os.path.join(path, 'moria.xlsx')

db = ms.mongo_connect()
dt = config['history'].get('date')

print('\n{:25}: {}'.format('rules db file', pmo))
writer = pd.ExcelWriter(pmo, engine='xlsxwriter')
for sheet in ['Lines', 'header']:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_history(collection, dt)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()

print('\n{:18}: {}'.format('phase3-bp', fn))

writer = pd.ExcelWriter(fn, engine='xlsxwriter')
sheet_list_0 = ['domain_ref', 'process_ref', 'MoM', 'requirements', 'interfaces', 'scm_tasks']
sheet_list_1 = ['process_ref', 'MoM', 'requirements', 'interfaces', 'scm_tasks']

sheet_list = sheet_list_0 if dt > '2021-01-03' else sheet_list_1
print(sheet_list)
print('\n{:18}: {}'.format('phase3-bp', fn))
for sheet in sheet_list:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_history(collection, dt)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()

print('\nmoria file')
print('\n{:18}: {}'.format('moria', mo))

writer = pd.ExcelWriter(mo, engine='xlsxwriter')
sheet_list_0 = ['SM' , 'SCM', 'SM_text', 'SCM_text']
sheet_list_1 = ['SM' , 'SCM']
sheet_list = sheet_list_0 if dt > '2021-01-03' else sheet_list_1
for sheet in sheet_list:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_history(collection, dt)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()

writer = pd.ExcelWriter(fr, engine='xlsxwriter')
sheet_list_0 = ['requirements', 'tasks']
print(sheet_list)
print('\n{:18}: {}'.format('requirements', fr))
dt = '2021-01-25'
for sheet in sheet_list_0:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_history(collection, dt)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()
