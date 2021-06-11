import os
import codecs
import pandas as pd
import numpy as np
import yaml
import time
import re

if __name__ == '__main__':
    print(125 * '=')
    pd.options.mode.chained_assignment = 'raise'
    start_a = time.time()

    with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    dt = config['history'].get('date')
    history = config['execution_params'].get('history')

    path = config['config'].get('path')
    f1 = os.path.join(path, config['config'].get('requirements'))
    f2 = os.path.join(path, config['config'].get('mrs-dev'))
    fo = os.path.join(path, config['config'].get('dev-cmp'))

    df_d1 = pd.read_excel(f1, sheet_name='דרישות')
    df_d2 = pd.read_excel(f2, sheet_name='RN')

    df_d1.rename(columns=config['realization_columns_map'], inplace=True)

    df_d1 = df_d1[['RN', 'Name']]
    df_d2 = df_d2[['RN', 'Name']]

    df_d1['Name'] = df_d1['Name'].str.strip()
    df_d2['Name'] = df_d2['Name'].str.strip()
    df_d2.set_index('RN', inplace=True)
    df_d2['RN'] = df_d2.index
    d2_map = df_d2.to_dict()

    df_d1['rn'] = df_d1.RN.map(d2_map.get('RN'))
    df_d1['dev-name'] = df_d1.RN.map(d2_map.get('Name'))

    df_d1['dev-name-x'] = df_d1['dev-name'].apply(
        lambda x: [character for character in x if character.isalnum()] if isinstance(x, str) else '')
    df_d1['dev-name-x'] = df_d1['dev-name-x'].apply(lambda x: ''.join(x).upper())

    df_d1['name-x'] = df_d1['Name'].apply(
        lambda x: [character for character in x if character.isalnum()] if isinstance(x, str) else '')
    df_d1['name-x'] = df_d1['name-x'].apply(lambda x: ''.join(x).upper())

    df_d1['match'] = df_d1.apply(lambda x: x['name-x'] in x['dev-name-x'], axis=1)

    writer = pd.ExcelWriter(fo, engine='xlsxwriter')
    df_d1.to_excel(writer, sheet_name='match-report')
    workbook = writer.book

    writer.save()
    print('{:23}: {:.3f}'.format('Total Execution time', time.time() - start_a))
    quit(1)
