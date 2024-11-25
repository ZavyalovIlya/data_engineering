# -*- coding: utf-8 -*-
"""Практика2_4.ipynb

"""

import json
import pickle

with open('fourth_task_products.json', 'rb') as f:
  products = pickle.load(f)

with open('fourth_task_updates.json', 'r', encoding='utf-8') as f:
  updates = json.load(f)

def update_product(price, method, param):
  if method == 'add':
    return price + param
  elif method =='sub':
    return price - param
  elif method == 'percent+':
    return price * (1 + param)
  elif method == 'percent-':
    return price * (1 - param)

updates_map = {}
for tovar in updates:
  updates_map[tovar['name']] = tovar

for tovar in products:
  if tovar['name'] in updates_map.keys():
    tovar['price'] = update_product(tovar['price'], updates_map['Инжир']['method'], updates_map['Инжир']['param'])

with open('fourth_task_output.pkl', 'wb') as fl:
  pickle.dump(products, fl)