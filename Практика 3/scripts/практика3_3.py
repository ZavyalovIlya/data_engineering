from bs4 import BeautifulSoup
import json
import pandas as pd

def xml_reader(filename):

  with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()

  star = BeautifulSoup(content, 'xml').star

  # read file content
  data = {el.name: el.get_text().strip() for el in star if el.name is not None}
  # convert numbers from str to float
  for el in ['radius', 'rotation', 'age', 'distance', 'absolute-magnitude']:
    data[el] = float(data[el].split(' ')[0])

  return data

data = []
for i in range(1, 217):
  data.append(xml_reader(f'{i}.xml'))

# export to json
with open('third_task_output.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False)

df = pd.DataFrame(data)
df.head()

# отсортируйте значения по одному из доступных полей
df_sorted = df.sort_values('radius')

# выполните фильтрацию по другому полю
df_filtered = df[df['age'] > 5]

# для одного выбранного числового поля посчитайте статистические характеристики
stats = {}
stats['min'] = int(df['distance'].min())
stats['max'] = int(df['distance'].max())
stats['avg'] = round(df['distance'].mean(), 2)
stats['med'] = int(df['distance'].median())

print(f'''statistics for 'distance' column:\nminimum distance is {stats['min']}\nmaximum ndistance is {stats['max']}
average distance is {stats['avg']}\nmedian distance is {stats['med']}''')

# для одного текстового поля посчитайте частоту меток
print({const : len(df[df['constellation']==const]) for const in df['constellation'].unique()})