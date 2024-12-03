from bs4 import BeautifulSoup
import json
import pandas as pd

def bool_map(val):
  tmp = {'-': False, '+': True, 'no': False, 'yes': True}
  return tmp[val]

type_dic = {'id': int, 'reviews': int, 'price': float, 'rating': float,
            'new': bool_map, 'exclusive': bool_map, 'sporty': bool_map}

def xml_reader(filename, list_xml):

  with open('127.xml', 'r', encoding='utf-8') as file:
          content = file.read()

  clothes = BeautifulSoup(content, 'xml').find_all('clothing')

  for item in clothes:
    # parsing xml

    # если элемент из xml находится в type_dic, то тип данных его значения меняется на соответствующий тип данных из type_dic
    # (для изменения str на int/float/bool исходя из содержателього смысла элемента)
    data = {el.name: type_dic[el.name](el.get_text().strip()) if el.name in type_dic.keys()
              # если элемента из xml нет в type_dic, то тип данных остается неизменным (str)
              else el.get_text().strip()
              # вышеперечисленные операции осуществить для всех элементов кроме None
              for el in item if el.name is not None}

    list_xml.append(data)

  return list_xml

list_xml = []
for i in range(1, 155):
  xml_reader(f'{i}.xml', list_xml)

# export to json
with open('fourth_task_output.json', 'w', encoding='utf-8') as file:
    json.dump(list_xml, file, ensure_ascii=False)

df = pd.DataFrame(list_xml)
df.head()

#	отсортируйте значения по одному из доступных полей
df_sorted = df.sort_values('price')

# выполните фильтрацию по другому полю
df_filtered = df[df['new'] == True]

# для одного выбранного числового поля посчитайте статистические характеристики
stats = {}
stats['min'] = df['rating'].min()
stats['max'] = df['rating'].max()
stats['avg'] = round(df['rating'].mean(), 2)
stats['med'] = df['rating'].median()

print(f'''statistics for 'distance' column:\nminimum rating is {stats['min']}\nmaximum rating is {stats['max']}
average rating is {stats['avg']}\nmedian rating is {stats['med']}''')

# для одного текстового поля посчитайте частоту меток
print({siz : len(df[df['size']==siz]) for siz in df['size'].unique()})