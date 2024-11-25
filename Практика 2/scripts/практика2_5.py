# -*- coding: utf-8 -*-
"""Практика2_5.ipynb

"""

import pandas as pd
import json
import msgpack
import pickle
import os

# Data of Patients ( For Medical Field )
# https://www.kaggle.com/datasets/tarekmuhammed/patients-data-for-medical-field
df_orig = pd.read_excel('patients_dataset.XLSX')

df = df_orig[['State', 'Sex', 'GeneralHealth', 'AgeCategory', 'HeightInMeters',
        'WeightInKilograms', 'HadHeartAttack', 'SmokerStatus', 'CovidPos']]

# заменим 1 на 'Yes' и 0 на 'No'
d_map = {1: 'Yes', 0: 'No'}
df = df.replace({'HadHeartAttack': d_map, 'CovidPos': d_map})
print(df.dtypes)

# статистика по столбцам с числовыми данными
num_stats = [
{col:
      {
      'max': df[col].max(),
      'min': df[col].min(),
      'sum': df[col].sum(),
      'avg': df[col].sum() / df[col].count(),
      'std': df[col].std()
      }}
for col in df.select_dtypes(['int64','float64']).columns]

# статистика по столбцам с текстовыми и булевым типами данных
obg_stats = [{col: {val: len(df[df[col] == val]) for val in df[col].unique()}}
    for col in df.select_dtypes(['object','bool']).columns]

with open('fifth_task_output.json', 'w', encoding='utf-8') as fl:
    json.dump(num_stats + obg_stats, fl, ensure_ascii=False)

df.to_csv('patients_data.csv')

df.to_json('patients_data.json')

with open('patients_data.msgpack', 'wb') as fl:
    msgpack.dump(df.to_dict(), fl)

with open('patients_data.pkl', 'wb') as fl:
  pickle.dump(df.to_dict(), fl)

csv_size = os.path.getsize("patients_data.csv")
json_size = os.path.getsize("patients_data.json")
msgpack_size = os.path.getsize("patients_data.msgpack")
pkl_size = os.path.getsize("patients_data.pkl")

sizes = {'csv': csv_size, 'json': json_size, 'msgpack': msgpack_size, 'pkl': pkl_size}

sorted_sizes = dict(sorted(sizes.items(), key=lambda item: item[1]))

print('File sizes from the smallest to the biggest (in bytes):')
for el in sorted_sizes:
  print(f'{el}: {sorted_sizes[el]}')