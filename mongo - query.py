
import os
import codecs
import pandas as pd
import numpy as np
import time
import warnings as warning
import workday as wd
import yaml
import mongo_services as ms
import re
import bp3

# pd.options.mode.chained_assignment = None
# isinstance(row.due_date, pd._libs.tslibs.nattype.NaTType)
# pd.bdate_range(start=start_d, end='20/10/2020', weekmask=weekmask, holidays=holidays, freq='C')
pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125*'=')

db         = ms.mongo_connect()

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

dt = True if config['history'].get('date') > '27/12/2020' else False
history = config['execution_params'].get('history')

if history:
    print('History execution')
    path = config['history'].get('path')

    fn = os.path.join(path, config['history'].get('name'))
    fr = os.path.join(path,    config['history'].get('pmo'))
    fo = os.path.join(path, config['history'].get('out'))
    fs = os.path.join(path, config['history'].get('skeleton'))
    fz = os.path.join(path, config['history'].get('consolidated'))
else:
    path = config['config'].get('path')

    fn = os.path.join(path, config['config'].get('name'))
    fr = os.path.join(path, config['config'].get('pmo'))
    fo = os.path.join(path, config['config'].get('out'))
    fs = os.path.join(path, config['config'].get('skeleton'))
    fz = os.path.join(path, config['config'].get('consolidated'))
    bi = os.path.join(path, config['config'].get('p3BI'))

df_bp = pd.read_excel(fo, sheet_name='Sheet1')
parent_list = df_bp['parent'].unique()
df_rules_x  = pd.read_excel(fr, sheet_name='Lines')

def create_cycle_map(topic='BP'):
    df_zz = df_rules_x.copy()
    df_zz = df_zz[['Rule', 'Name']]

    df_zz = df_zz[df_zz['Rule'] == topic]
    df_zz = df_zz.iloc[1:]
    lc_list = df_zz.Name.to_list()
    lc_cat = pd.Categorical(lc_list, categories=lc_list, ordered=True)
    return lc_cat

def db_query(collection=None, q=None):
    collect = ms.get_collection(db, collection)
    df_h = pd.DataFrame()
    data = []

    x = collect.find(q)
    for r in x:
        data.append(r.get('results'))

    df_h = df_h.append(data, True)
    #   df_h['indexDT'] = r.get('index')
    return df_h

def previous_step(cat=None, c_status=None):
    return cat[
        max(
            0,
            np.where(c_status == cat)[0][0] - 1
        )
    ]

start = time.time()
print(125*'*')
collection = ms.get_collection(db, 'BI')
df_h = ms.retrieve_all(collection)
df_h['parent'] = df_h['RN'].apply(lambda x: '.'.join(x.split('.')[0:4]))
df_h['Start_Date'] = pd.to_datetime(df_h['Start_Date'])
df_h['Finish_Date'] = pd.to_datetime(df_h['Finish_Date'])

for p in parent_list[1:]:
    df = df_bp[(df_bp['row_type'].isin(['parent'])) & (df_bp['parent'].isin([p]))]  # parent's row
    p_rule      = df.rule.values[0]
    lc_category = create_cycle_map(topic=p_rule)

    p_status = df.get('status').values[0]
    step     = previous_step(cat=lc_category, c_status=p_status)

    df_x = df_h[(df_h['parent'] == p) & (df_h.Task_Name.str.contains(step))]

    if len(df_x) > 0:
        df_dt_max = df_x['Finish_Date'].min()
        # print('{} min date: {} step: {}'.format(p, df_dt_max.strftime('%d/%m/%Y'), step))

print('{:50}: {:05.2f}'.format('DB exec time', time.time() - start))

def xyz():
    s_str_0 = p + '.*'
    s_str_1 = '.*' + step + '.*'

    query = {
        '$and': [
            {'results.RN': {'$regex': s_str_0}},
            {'results.Task_Name': {'$regex': s_str_1}},
            {'results.row_type': 'rule'}
        ]
    }
    df = db_query(collection='BI', q=query)
