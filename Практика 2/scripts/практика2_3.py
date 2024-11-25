# -*- coding: utf-8 -*-
"""Практика2_3.ipynb

"""

import json
import pandas as pd
import msgpack
import os

df = pd.read_json('third_task.json')

df_sum = df.groupby(['name']).sum()
df_count = df.groupby(['name']).count()
df_max = df.groupby(['name']).max()
df_min = df.groupby(['name']).min()

tovar_stats = [{
        'name': tovar,
        'avg': df_sum.loc[tovar]['price'] / df_count.loc[tovar]['price'],
        'max': float(df_max.loc[tovar]['price']),
        'min': float(df_min.loc[tovar]['price'])
      }
 for tovar in df['name'].unique()
]

with open('third_task_output1.json', 'w', encoding='utf-8') as fl:
    json.dump(tovar_stats, fl, ensure_ascii=False)

with open('third_task_output2.msgpack', 'wb') as fl:
    msgpack.dump(tovar_stats, fl)

json_size = os.path.getsize("third_task_output1.json")
msgpack_size = os.path.getsize("third_task_output2.msgpack")

print(f'json file size = {json_size} bytes')
print(f'msgpack file size = {msgpack_size} bytes')
print(f'the difference {json_size - msgpack_size} bytes')