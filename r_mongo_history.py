import os
import codecs
import pandas as pd
import warnings as warning
import yaml
import mongo_services as ms
from pymongo import MongoClient

pd.options.mode.chained_assignment = 'raise'  # pd.options.mode.chained_assignment = None
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

client = MongoClient(port=27017)              # client = MongoClient('mongodb://localhost:27017/')
db = client['Phase3']

print('risk table reporting history')
collection = ms.get_collection(db, 'risk')
ms.print_history_index(collection)

print('requirements table reporting history')
collection = ms.get_collection(db, 'requirements')
ms.print_history_index(collection)
