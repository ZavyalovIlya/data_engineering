from bs4 import BeautifulSoup
import json
import pandas as pd

def html_reader(filename, list_html):

  with open(filename, 'r', encoding='utf-8') as file:
      content = file.read()

  soup = BeautifulSoup(content, 'html.parser')
  items = soup.find_all('div', attrs={'class': 'product-item'})

  for item in items:
    data = {}
    data['id'] = int(item.find_all('a')[0]['data-id'])
    data['link'] = item.find_all('a')[1]['href']
    data['img'] = item.img['src']
    data['name'] = item.span.get_text().strip()
    data['price'] = int(item.price.get_text().replace('₽', '').replace(' ', '').strip())
    data['bonus'] = int(item.strong.get_text().strip().split(' ')[2])
    for dop in item.ul.find_all('li'):
      res = dop.get_text().strip().split(' ')[0]
      try: data[dop['type']] = int(res)
      except: data[dop['type']] = res
    list_html.append(data)

  return list_html

html_data = []

for i in range(1, 32):
  html_reader(f'{i}.html', html_data)

# export to json
with open('second_task_output.json', 'w', encoding='utf-8') as file:
    json.dump(html_data, file, ensure_ascii=False)

df = pd.DataFrame(html_data)
df.head()

# отсортируйте значения по одному из доступных полей
df_sorted = df.sort_values('price')

# выполните фильтрацию по другому полю
df_filtered = df[df['bonus'] > 1800]

# для одного выбранного числового поля посчитайте статистические характеристики
stats = {}
stats['min'] = int(df['sim'].min())
stats['max'] = int(df['sim'].max())
stats['avg'] = round(df['sim'].mean(), 2)
stats['med'] = int(df['sim'].median())

print(f'''statistics for 'sim' column:\nminimum number of sims is {stats['min']}\nmaximum number of sims is {stats['max']}
average number of sims is {stats['avg']}\nmedian number of sims is {stats['med']}''')

# для одного текстового поля посчитайте частоту меток
print({matr : len(df[df['matrix']==matr]) for matr in df['matrix'].dropna().unique()})