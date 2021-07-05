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
rsk  = os.path.join(path, 'h_risk.xlsx')
fn   = os.path.join(path, config['history'].get('name'))
mo   = os.path.join(path, 'moria.xlsx')

db = ms.mongo_connect()
dt = '2021-01-10'

print('\n{:25}: {}'.format('rules db file', rsk))
writer = pd.ExcelWriter(rsk, engine='xlsxwriter')
for sheet in ['risk']:
    collection = ms.get_collection(db, sheet)
    df         = ms.retrieve_history(collection, dt)
    workbook   = writer.book
    df.to_excel(writer, sheet_name=sheet)
    print(df.shape)
writer.save()
