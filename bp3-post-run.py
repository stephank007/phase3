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

    path = config['config'].get('path')
    fn = os.path.join(path, config['config'].get('out'))
    sl = os.path.join(path, config['config'].get('shortlist'))
    fo = os.path.join(path, config['config'].get('late'))

    df_out = pd.read_excel(fn, sheet_name='Sheet1')
    df_srl = pd.read_excel(sl, sheet_name='RN')

    df_out.set_index('xRN', inplace=True)
    for index, row in df_srl.iterrows():
        parent = df_out.loc[row.RN, 'RN']

    quit(0)

    # df_out = df_out.rename(columns=config['dfP'])
    # df_out = df_out[config['dfP_columns']]

    writer = pd.ExcelWriter(fo, engine='xlsxwriter')
    workbook = writer.book
    df_out.to_excel(writer, sheet_name='Task_Table1')
    writer.save()
