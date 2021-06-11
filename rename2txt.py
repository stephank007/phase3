
import os
import shutil as sh
import codecs
import pandas as pd
import warnings as warning
import yaml

# pd.options.mode.chained_assignment = None
# isinstance(row.due_date, pd._libs.tslibs.nattype.NaTType)
# pd.bdate_range(start=start_d, end='20/10/2020', weekmask=weekmask, holidays=holidays, freq='C')
pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125*'=')

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('burn')
os.chdir(path)

for r, d, f in os.walk(path):
    print(r, d)
    for file in f:
        for ext in ['.py', '.yaml', '.css']:
            if ext  in file:
                root = os.path.splitext(file)
                sh.move(file, root[0] + '.txt')