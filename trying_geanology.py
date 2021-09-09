import pandas as pd
import codecs
import yaml
import os
import json
import string

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

def search(dataframe, item):
  mask = (dataframe.applymap(lambda x: isinstance(x, str) and item in x)).any(1)
  return dataframe[mask]

path = config['config'].get('path')
fz   = os.path.join(path, config['config'].get('dev-cmp'))
#jd = os.path.join(path, config['config'].get('json_data'))

# df = pd.read_html('https://search.crarg.org/crarg-holocaust-era?utf8=?&surname_filter=sounds_like&q=FRAJERMAUER', encoding='utf-8')
writer = pd.ExcelWriter(fz, engine='xlsxwriter')
eliasz = {'key': 'value'}

translation_table = dict.fromkeys(map(ord, '!@#$.-'), None)
line = '.moshe-'
line = line.translate(translation_table)
print(line)

# df = df[2]
# df.to_excel(writer, sheet_name='FRAJERMAUER')
s = 'RAJSMAN'
df = pd.read_html('https://search.crarg.org/crarg-holocaust-era?utf8=?&surname_filter=sounds_like&q=RAJSMAN', encoding='utf-8')
# f = open(jd, 'w')
for d in range(len(df)):
    df_n = df[d]
    df_n = search(df_n, 'Eliasz')
    if len(df_n) > 0:
        f.write('=====================\n')
        line = [col + ':' + str(df_n[col].values[0]) for col in df_n.columns]
        for i in range(len(line)):
            l1 = line[i].split(':')
            for x in range(len(l1)):
                key   = l1[0].translate(translation_table)
                value = l1[1].translate(translation_table)
                eliasz.update({key : value})
        for key, value in eliasz.items():
            print(key, ': ', value)
            f.write('{}: {}\n'.format(key, value))
        eliasz.update(eliasz)
        # df_elisz = pd.concat([df_eliasz, df_n], ignore_index=True)

f.close()
quit(0)

