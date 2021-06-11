import os
import codecs
import yaml
import pandas as pd
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-b', required=True)
args = parser.parse_args()
sheet_name = args.b
sheet_name_txt = args.b + '_text'

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)  

today    = pd.to_datetime('today')

fn = os.path.join(config['config'].get('path'), config['config'].get('moria'))
fo = os.path.join(config['config'].get('path'), config['config'].get('reconcile'))
bp = os.path.join(config['config'].get('path'), config['config'].get('out'))

df     = pd.read_excel(fn, sheet_name=sheet_name)
cn_start = [i for (i, j) in enumerate(df.columns) if j == 'רמד יישום'][0]
cn_end   = [i for (i, j) in enumerate(df.columns) if j == 'שגיב שרביט'][0] + 1

df = df.rename(columns={'תהליך': 'process_id'})
df = pd.melt(df, id_vars='process_id', value_vars=df.iloc[:, np.r_[cn_start:cn_end]], value_name='completed')

df['idx'] = df.index
df['gantt_name'] = df['variable'].map(config['moria_convert'])

def reconcile():
    """ reconciliation file processing """
    df_bp = pd.read_excel(bp, sheet_name='Sheet1')
    df_bp = df_bp[['RN', 'process_id', 'Name', '% Complete']]

    df['key']    = df['process_id']    + '-' + df['gantt_name']
    df_bp['key'] = df_bp['process_id'] + '-' + df_bp['Name']
    df.set_index('key', inplace=True)
    dict_moria  = df.to_dict()
    df_bp['moria_status'] = df_bp['key'].map(dict_moria.get('completed'))
    df_bp = df_bp[~df_bp['moria_status'].isna()]
    df_bp['moria_status'] = df_bp['moria_status'].apply(lambda x: 1 if x in [43] else 0)

    df_bp['reconcile'] = df_bp.apply(lambda x: 'reconcile' if x['% Complete'] != x['moria_status'] else None, axis=1)
    df_bp = df_bp[~df_bp['reconcile'].isna()]
    df_bp = df_bp[~df_bp['process_id'].isna()]

    df_bp = df_bp[['RN', 'process_id', 'Name', 'moria_status', '% Complete', 'reconcile']]

    writer = pd.ExcelWriter(fo, engine='xlsxwriter')
    workbook = writer.book
    df_bp.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    ''' reconciliation end '''

reconcile()
