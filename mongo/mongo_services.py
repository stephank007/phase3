from pymongo import MongoClient
import os
import yaml
import codecs
import pandas as pd
import math


with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

def get_date():
    ts  = pd.Timestamp.now()
    fts = '{:02d}-{:02d}-{:02d}'.format(ts.year, ts.month, ts.day)
    return fts
def get_time():
    ts  = pd.Timestamp.now()
    fts = '{:02d}:{:02d}:{:02d}'.format(ts.hour, ts.minute, ts.second)
    return fts
def is_null(data):
    if isinstance(data, (int, float)):
        if math.isnan(data):
            return True
    if data is None:
        return True
    else:
        return False

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
def mongo_connect():
    try:
        client = MongoClient(port=27017)  # client = MongoClient('mongodb://localhost:27017/')
        db = client['P3']
    except Exception as e:
        print("Failed to connect to mongoDB: ", e)

    return db

def get_collection(db, collection_name):
    return db[collection_name]

def add_records(df, collection):
    df['index_date'] = get_date()
    data_dict = df.to_dict("records")
    for record in data_dict:
        collection.insert_one({"index": get_date(), "results": record})


def retrieve_history(requirements, d_index):
    df_h = pd.DataFrame()
    data = []

    x = requirements.find({"index": d_index})
    for r in x:
        data.append(r.get('results'))
    df_h = df_h.append(data, True)
    # df_h['indexDT'] = r.get('index')
    return df_h

def retrieve_all(requirements):
    df_h = pd.DataFrame()
    df_d = pd.DataFrame()
    data = []
    d_data = []

    x = requirements.find()
    for r in x:
        data.append(r.get('results'))
        d_data.append(r.get('index'))
    df_h = df_h.append(data, True)
    df_d = df_d.append(d_data, True)
    df_h['d_index'] = df_d[0]
    return df_h

def delete_history(requirements, d_index):
    my_query = {"index" : d_index}
    x = requirements.delete_many(my_query)
    print(x.deleted_count, "documents deleted for: ", d_index)
