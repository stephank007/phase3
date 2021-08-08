
import os
import shutil as sh
import codecs
import pandas as pd
import warnings as warning
import yaml

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125*'=')

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

print(os.getcwd())
path = '../../khalid'
os.chdir(path)
print(os.getcwd())

for r, d, f in os.walk('.'):
    print(r, d)
    for file in f:
        if '.txt' in file:
            root = os.path.splitext(file)
            sh.move(file, root[0] + '.py')
