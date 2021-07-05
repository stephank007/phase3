import os
import codecs
import pandas as pd
import warnings as warning
import yaml
import mongo_services as ms
from pymongo import MongoClient

pd.options.mode.chained_assignment = 'raise'                    # pd.options.mode.chained_assignment = None
warning.filterwarnings('ignore')
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

def get_history_index(requirements):
    data = []
    for idx in requirements.find().distinct('index'):
        data.append(idx)

    return data

def print_history_index(requirements):
    i = 1
    print('Requirements Collection indices:')
    for d in get_history_index(requirements):
        print('\t{:02d}: {}'.format(i, d))
        i += 1

def retrieve_all(requirements):
    df_h   = pd.DataFrame()
    df_d   = pd.DataFrame()
    data   = []
    d_data = []

    x = requirements.find()
    for r in x:
        data.append(r.get('results'))
        d_data.append(r.get('index'))
    df_h = df_h.append(data, True)
    df_d = df_d.append(d_data, True)
    df_h['d_index'] = df_d[0]
    return df_h

path = config['config'].get('path')
rsk  = os.path.join(path, config['config'].get('risk-out'))

writer     = pd.ExcelWriter(rsk, engine='xlsxwriter')
workbook   = writer.book
sheet_name = 'risk'

client = MongoClient(port=27017)                                # client = MongoClient('mongodb://localhost:27017/')
db     = client['Phase3']

print('\n{:25}: {}'.format('rules db file', rsk))

collection = ms.get_collection(db, sheet_name)
print_history_index(collection)

df = retrieve_all(collection)
df.to_excel(writer, sheet_name=sheet_name)
print(df.shape)

writer.save()
""" update many - quarantine"""
"""
    result = collection.update_many(
        {'index' : '2021-01-10'},
        {
            '$set' : {'index' : '2020-09-01'}
        }
    )

    result = collection.update_many(
        {'index' : '2021-07-01'},
        {
            '$set' : {'index' : '2021-01-10'}
        }
    )
"""