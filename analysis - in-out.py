import os
import codecs
import pandas as pd
import yaml
import time
from pymongo import MongoClient

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

def get_collection(db_name, collection_name):
    return db_name[collection_name]

if __name__ == '__main__':
    print(125 * '=')
    pd.options.mode.chained_assignment = 'raise'
    start_a = time.time()

    with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    path = config['config'].get('path')

    client = MongoClient(port=27017)  # client = MongoClient('mongodb://localhost:27017/')
    sheet_name = 'shortlist'
    db = client['Dev']

    collection = get_collection(db, sheet_name)
    print_history_index(collection)

    df = retrieve_all(collection)
    baseline_dt = '2021-08-18'
    current_dt  = df['d_index'].max()
    df['Duration'] = df['Duration'].apply(lambda x: x.split(' ')[0]).astype(float)

    f3 = os.path.join(path, config['config'].get('requirements'))
    fo = os.path.join(path, config['config'].get('in-out'))

    writer = pd.ExcelWriter(fo, engine='xlsxwriter')
    workbook = writer.book

    df_f3 = pd.read_excel(f3, sheet_name='דרישות')  # requirements (all)

    df_f1 = df[df['d_index'] == current_dt]
    df_f2 = df[df['d_index'] == baseline_dt]
    df_f1 = df_f1.copy()
    df_f2 = df_f2.copy()

    df_f1.rename(columns=config['realization_columns_map'], inplace=True)
    df_f2.rename(columns=config['realization_columns_map'], inplace=True)
    df_f3.rename(columns=config['realization_columns_map'], inplace=True)

    df_f3 = df_f3.rename(columns={'logmar_process' : 'process_id'})
    df_f3.set_index('xRN', inplace=True)
    df_f3 = df_f3.to_dict()

    df_f1['s_rule'] = df_f1['s_rule'].str.strip()
    df_f2['s_rule'] = df_f2['s_rule'].str.strip()

    df_f1['Name']   = df_f1['Name'].str.strip()
    df_f2['Name']   = df_f2['Name'].str.strip()

    df_f1['rule'] = df_f1['s_rule'].apply(lambda x: config['s_rule_map'].get(x))
    df_f2['rule'] = df_f2['s_rule'].apply(lambda x: config['s_rule_map'].get(x))

    df_f1['domain_id']  = df_f1.xRN.map(df_f3.get('domain_id'))
    df_f1['process_id'] = df_f1.xRN.map(df_f3.get('process_id'))

    df_f2['domain_id']  = df_f2.xRN.map(df_f3.get('domain_id'))
    df_f2['process_id'] = df_f2.xRN.map(df_f3.get('process_id'))

    df_f1 = df_f1[['xRN', 'domain_id', 'process_id', 'Name', 'rule', 'Duration']]
    df_f2 = df_f2[['xRN', 'domain_id', 'process_id', 'Name', 'rule', 'Duration']]

    df_f1.columns = ('{}_{}'.format('L', c) for c in df_f1.columns)
    df_f2.columns = ('{}_{}'.format('R', c) for c in df_f2.columns)

    df_cmp = df_f1.merge(df_f2, left_on='L_xRN', right_on='R_xRN', how='outer')

    for index, row in df_cmp.iterrows():
        if isinstance(row.L_xRN, float):
            df_cmp.loc[index, 'in-out'] = 'out'
        elif isinstance(row.R_xRN, float):
            df_cmp.loc[index, 'in-out'] = 'in'

    df_cmp = df_cmp[df_cmp['in-out'].notnull()]
    df_cmp.to_excel(writer, sheet_name='in-out')
    writer.save()

    baseline_duration = df_f1.L_Duration.sum()
    current_duration  = df_f2.R_Duration.sum()
    print('\nbaseline: {} current: {}'.format(baseline_dt, current_dt))
    print('duration difference: {:.1f}'.format(current_duration - baseline_duration))
