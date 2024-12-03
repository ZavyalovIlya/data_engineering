from bs4 import BeautifulSoup
import json
import pandas as pd

def parse(line, position):
  return line[position].get_text().replace('\n', '').split(':')[1].strip()

def html_reader(filename):

  with open(filename, 'r', encoding='utf-8') as file:
    content = file.read()

  soup = BeautifulSoup(content, 'html.parser')

  data = {}

  chess = soup.find('div', attrs={'class': 'chess-wrapper'})

  spans_empty = chess.find_all('span', attrs={'class': ''})

  data['type'] = parse(spans_empty, 0)
  data['id'] = int(chess.find_all('h1')[0]['id'])
  data['tournament'] = parse(chess.find_all('h1'), 0)
  tmp = chess.find_all('p')[0].get_text().replace('\n', '').split('Начало:')
  data['city'] = tmp[0].split(':')[1].strip()
  data['date'] = tmp[1].strip()
  data['n_rounds'] = int(parse(chess.find_all('span', attrs={'class': 'count'}), 0))
  data['max_time_minuts'] = int(parse(chess.find_all('span', attrs={'class': 'year'}), 0).split(' ')[0])
  data['min_rating'] = int(parse(spans_empty, 1))
  data['rating'] = float(parse(spans_empty, 2))
  data['views'] = int(parse(spans_empty, 3))
  data['img'] = chess.img['src']

  return data

data = []
for i in range(2, 88):
  data.append(html_reader(f'{i}.html'))

# export to json
with open('first_task_output.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False)

df = pd.DataFrame(data)
df.head()

# отсортируйте значения по одному из доступных полей
df_sorted = df.sort_values('min_rating')

# выполните фильтрацию по другому полю (запишите результат отдельно)
df_filtered = df[df['views'] > 20000]

# для одного выбранного числового поля посчитайте статистические характеристики
stats = {}
stats['min'] = df['rating'].min()
stats['max'] = df['rating'].max()
stats['avg'] = round(df['rating'].mean(), 2)
stats['med'] = df['rating'].median()

print(f'''statistics for 'rating' column:\nminimum rating is {stats['min']}\nmaximum rating is {stats['max']}
average rating is {stats['avg']}\nmedian rating is {stats['med']}''')

# для одного текстового поля посчитайте частоту меток
print({comp : len(df[df['type']==comp]) for comp in df['type'].unique()})