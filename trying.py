import os
import codecs
import yaml
import shutil
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

def move_files(path_x):
    for root, dirs, files in os.walk(path_x):
        path_y = root.split(os.sep)
        # print((len(path) - 1) * '---', os.path.basename(root))
        for file in files:
            name, extension = os.path.splitext(file)
            if extension:
                yield os.path.join(root, file)

path = config['config'].get('path')
result = move_files(path)
for r in result:
    print(r)
