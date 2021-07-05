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
pmo  = os.path.join(path, config['config'].get('dev-cmp'))

db = ms.mongo_connect()
writer = pd.ExcelWriter(pmo, engine='xlsxwriter')
dt = '2020-05-04'

for sheet in ['Requirements']:
    collection = ms.get_collection(db, sheet)
    updateResult = collection.update_many(
        {'index': dt + ':x1'},
        {
            '$set': {'index': dt}
        }
    )
    df = ms.retrieve_history(collection, dt)
    print(df.shape)

quit(0)
workbook = writer.book
df.to_excel(writer, sheet_name=sheet)
writer.save()