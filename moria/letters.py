import os
import re
import codecs
import yaml
import pandas as pd
import numpy as np

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

history = config['execution_params'].get('history')
if history:
    print('History Execution')
    path = config['history'].get('path')

    fn = os.path.join(path, config['history'].get('moria'))
    lt = os.path.join(path, config['history'].get('letters'))
else:
    path = config['config'].get('path')

    fn = os.path.join(path, config['config'].get('moria'))
    lt = os.path.join(path, config['config'].get('letters'))

df_sm = pd.read_excel(fn, sheet_name='SM')
df_sc = pd.read_excel(fn, sheet_name='SCM')
df_sm_text = pd.read_excel(fn, sheet_name='SM_text')
df_sc_text = pd.read_excel(fn, sheet_name='SCM_text')

df_sm = df_sm.rename(columns={'תהליך': 'process_id'})
df_sc = df_sc.rename(columns={'תהליך': 'process_id'})
df_sm_text = df_sm_text.rename(columns={'תהליך': 'process_id'})
df_sc_text = df_sc_text.rename(columns={'תהליך': 'process_id'})

cn_start_1 = [i for (i, j) in enumerate(df_sm.columns) if j == 'אבי פרישמן'][0]
cn_end_1 = [i for (i, j) in enumerate(df_sm.columns) if j == 'שגיב שרביט'][0] + 1

cn_start_2 = [i for (i, j) in enumerate(df_sc.columns) if j == 'אבי פרישמן'][0]
cn_end_2 = [i for (i, j) in enumerate(df_sc.columns) if j == 'שגיב שרביט'][0] + 1

df_sm = pd.melt(df_sm, id_vars='process_id', value_vars=df_sm.iloc[:, np.r_[cn_start_1:cn_end_1]],
                value_name='completed')
df_sc = pd.melt(df_sc, id_vars='process_id', value_vars=df_sc.iloc[:, np.r_[cn_start_2:cn_end_2]],
                value_name='completed')

df_sm_text = pd.melt(df_sm_text, id_vars='process_id', value_vars=df_sm_text.iloc[:, np.r_[cn_start_1:cn_end_1]],
                     value_name='text')
df_sc_text = pd.melt(df_sc_text, id_vars='process_id', value_vars=df_sc_text.iloc[:, np.r_[cn_start_2:cn_end_2]],
                     value_name='text')

df_all = pd.concat(
    [df_sm, df_sc],
    ignore_index=True,
    sort=False
)

df_all_text = pd.concat(
    [df_sm_text, df_sc_text],
    ignore_index=True,
    sort=False
)

start_col = 'אבי פרישמן'
end_col   = 'שגיב שרביט'

df_all['key'] = df_all['process_id'] + '-' + df_all['variable']
df_all_text['key'] = df_all_text['process_id'] + '-' + df_all_text['variable']

df_all.sort_values(by=['process_id'], inplace=True)
df_all_text.sort_values(by=['process_id'], inplace=True)
df_all_text.set_index('key', inplace=True)
df_all_text_dict = df_all_text.to_dict()

df_all['text'] = df_all.key.map(df_all_text_dict.get('text'))
def find_exception(text):
    pattern_1 = '\s'
    pattern_2 = '^[^0-9]*\*\*\*'
    text = re.split(pattern_1, text)
    # print('\n', len(text), ' --- ', text)
    for word in text:
        # print(end='.')
        match = re.match(pattern_2, word)
        if match:
            # print('match found: {}'.format(word))
            return True
    return False

# for index, row in df_all.iterrows():
#     df_all.loc[index, 'exception'] = find_exception(str(row.text))

df_all = df_all[(df_all['completed'] == 42)]

df_letters = df_all.copy()
df_letters.sort_values(by=['process_id'], inplace=True)
df_letters.index = np.arange(0, len(df_letters))

def waiting(text):
    pattern_1 = '\s'
    pattern_2 = '[_]\d+[/|.]\d+'
    pattern_3 = '^[^0-9]*\d+[/|.]\d+'
    pattern_4 = '\D*\d{1,2}(/|.)\d{1,2}'
    pattern_5 = '\D'                      # split the non digit same as ^[^0-9]* if any
    pattern_6 = '\d{1,2}(/|.)\d{1,2}'     # split the digit part if any

    text = re.split(pattern_1, text)
    date_load = []
    if len(text) > 0:
        for word in text:
            match = re.match(pattern_4, word)
            if match:
                # print('match found: {}'.format(word))
                special = [True for s in re.split(pattern_6, word) if s == '_']  # get the special character part
                date = re.split(pattern_5, word)                                 # get the date part
                day = [d for d in date if d.isdigit()]
                if len(day) > 1:
                    date = f'{str(day[0]):0>2}' + '/' + f'{str(day[1]):0>2}' + '/2020'
                    # print('date is: {}'.format(date))
                    date = pd.to_datetime(date, format='%d/%m/%Y')
                if special:
                    return date
                else:
                    date_load.append(date)

    return [max(date_load) if len(date_load) > 0 else None]

for index, row in df_letters.iterrows():
    df_letters.loc[index, 'waiting_date'] = waiting(str(row.text))

df_letters = df_letters[~df_letters['waiting_date'].isnull()]
df_letters['waiting_date'] = pd.to_datetime(df_letters['waiting_date'])
df_letters['waiting_date'] = df_letters['waiting_date'].dt.strftime('%d/%m/%Y')

df_letters['p_name'] = df_letters['process_id'].apply(lambda x: x.split(':')[1])
df_letters['p_id'] = df_letters['process_id'].apply(lambda x: x.split(':')[0])
df_letters = df_letters[['process_id', 'p_id', 'p_name', 'variable', 'waiting_date']]
# df_letters['variable'] = df_letters.variable.map(config.get('temsha_convert'))
# df_letters['Name'] = df_letters.Name.map(config.get('temsha_convert'))
df_letters.rename(columns={
    'p_id': 'מזהה',
    'p_name': 'תהליך',
    'variable': 'אחראי להתייחסות',
    'waiting_date': 'הופץ להתייחסות'
}, inplace=True)

writer = pd.ExcelWriter(lt, engine='xlsxwriter')
workbook = writer.book
df_letters.to_excel(writer, sheet_name='Sheet1')
writer.save()
