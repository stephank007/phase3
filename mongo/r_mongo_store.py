import os
import codecs
import pandas as pd
import warnings as warning
import yaml
import mongo_services as ms

pd.options.mode.chained_assignment = 'raise'  # pd.options.mode.chained_assignment = None
warning.filterwarnings('ignore')
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
p3 = os.path.join(path, config['config'].get('risk'))

df_bp = pd.read_excel(p3, sheet_name='רשימת סיכונים')

df_dict = {
    'risk'    : df_bp,
}

db = ms.mongo_connect()
for name in df_dict:
    collection = ms.get_collection(db, name)
    df         = df_dict.get(name)
    print('\n', name, df.shape)
    ms.delete_history(collection, ms.get_date())
    ms.add_records(df, collection)

    ms.print_history_index(collection)
