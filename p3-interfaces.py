
import os
import codecs
import pandas as pd
import numpy as np
import time
import warnings as warning
import workday as wd
import yaml
import re

# pd.options.mode.chained_assignment = None
# isinstance(row.due_date, pd._libs.tslibs.nattype.NaTType)
# pd.bdate_range(start=start_d, end='20/10/2020', weekmask=weekmask, holidays=holidays, freq='C')
pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')

start_s = time.time()

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
path = config['config'].get('path')

fn = os.path.join(path, config['config'].get('name'))
fs = os.path.join(path, config['config'].get('out'))
fo = os.path.join(path, config['config'].get('int-out'))

df_sk = pd.read_excel(fs, sheet_name='Sheet1')
df    = pd.read_excel(fn, sheet_name='interfaces')

df           = df[~df['dependency'].isnull()]
df['groups'] = df['dependency'].str.split(',')
df['RN']     = df['process']
df           = df[['RN', 'process', 'Name', 'dependency', 'groups', 'function_requirement_reference', 'process_id']]

df_pid = df_sk.copy()
df_pid.set_index('RN', inplace=True)
df_pid_dict = df_pid.to_dict()

df_sk            = df_sk[df_sk['row_type'] == 'parent']
df_sk['r_group'] = df_sk['RN'].apply(lambda x: '.'.join(x.split('.')[0:3]))
df_sk            = df_sk[['RN', 'Name', 'r_group', 'row_type', 'rule']]
df_sk            = df_sk.rename(columns={'RN': 'm_RN'})

df_req = pd.DataFrame()
for index, row in df.iterrows():
    df_x = pd.DataFrame()
    for g in row.groups:
        g = g.strip()
        p_rn = row.process
        df_x = df_sk[df_sk['r_group'] == g]
        if df_x.shape[0] > 0:
            df_x = df_x.copy()
            df_x['rn'] = np.arange(df_x.shape[0]) + 1
            df_x['RN'] = df_x.apply(lambda x: p_rn + '-' + x.m_RN, axis=1)
            df_x['process_id'] = df_x['m_RN'].map(df_pid_dict.get('process_id'))
        df_req = df_req.append(df_x)

df = pd.concat([df, df_req], ignore_index=True)
df_bp = df.copy()
df_bp = df_bp[['RN', 'r_group', 'rule', 'm_RN', 'function_requirement_reference', 'process_id', 'Name']]
df_bp.sort_values(by=['RN'], inplace=True)

writer = pd.ExcelWriter(fo, engine='xlsxwriter')
df_bp.to_excel(writer, sheet_name='dfP')
worksheet = writer.sheets['dfP']
writer.save()
