
"""
В работе осуществляется парсинг html страниц с научными статьями сайта "Google Академия" (https://scholar.google.com/) по запросу "machine learning" (https://scholar.google.com/scholar?hl=ru&as_sdt=0%2C5&q=machine+learning&oq=)
"""

from bs4 import BeautifulSoup
import json
import pandas as pd
import requests
import time

# функция для парсинга страниц-каталогов
def html_reader(filename, list_html):

  soup = BeautifulSoup(filename, 'html.parser')

  items = soup.find_all('div', attrs={'class': 'gs_r gs_or gs_scl'})

  for item in items:
    data = {}
    data['id'] = item['data-aid']
    data['title'] = item.find('a', attrs={'id': data['id']}).get_text()
    tmp = item.find('div', attrs={'class': 'gs_a'}).get_text().replace('\xa0', '').split('- ')
    data['citations'] = int(item.find('div', attrs={'class': 'gs_fl gs_flb'}).find_all('a')[2].get_text().split(': ')[-1])
    data['authors'] = tmp[0].strip()
    data['n_authors'] = len(data['authors'].split(', '))
    data['year'] = int(tmp[-2].split(', ')[-1].strip())
    data['source'] = tmp[-1].strip()
    data['link'] = item.find('a', attrs={'id': data['id']})['href']

    list_html.append(data)

  return list_html

# для парсинга с помощью запросов
'''
articles = []
for i in range(0, 21):
  req = requests.get(f'https://scholar.google.com/scholar?start={i*10}&q=machine+learning&hl=ru&as_sdt=0,5')
  time.sleep(4)
  html_reader(req.content, articles)
'''

# для парсинга с помощью предзагруженных страниц
articles = []
for i in range(1, 6):

  with open(f'web_page{i}.html', 'r', encoding='utf-8') as file:
      content = file.read()

  html_reader(content, articles)

# функция для парсинга страниц, посвященных отдельным элементам (статьям)
def html_parser_single(filename, list_html):

  soup = BeautifulSoup(filename, 'html.parser')
  item = soup.find('div', attrs={'id': 'gsc_vcpb'})

  data = {}

  data['title'] = item.find('div', attrs={'id': 'gsc_oci_title'}).get_text()

  for el in item.find_all('div', attrs={'class': 'gs_scl'}):
    name = el.find('div', attrs={'class': 'gsc_oci_field'}).get_text()
    if name != 'Всего ссылок':
      data[name] = el.find('div', attrs={'class': 'gsc_oci_value'}).get_text()
    else:
      data['Цитирований'] = int(el.find('div', attrs={'class': 'gsc_oci_value'}).find('a').get_text().split(': ')[1])
      break

  data['Колво авторов'] = len(data['Авторы'].split(', '))

  return list_html.append(data)

articles_single = []
for i in range(1, 21):

  with open(f'single{i}.html', 'r', encoding='utf-8') as file:
      content = file.read()

  html_parser_single(content, articles_single)

# export to json
with open('fifth_task_output_multiple.json', 'w', encoding='utf-8') as file:
    json.dump(articles, file, ensure_ascii=False)

# export to json
with open('fifth_task_output_single.json', 'w', encoding='utf-8') as file:
    json.dump(articles_single, file, ensure_ascii=False)

df = pd.DataFrame(articles)
df.head()

# отсортируйте значения по одному из доступных полей
df_sorted = df.sort_values('year')

# выполните фильтрацию по другому полю
df_filtered = df[df['citations'] >= 3000]

# для одного выбранного числового поля посчитайте показатели статистики
stats = {}
stats['min'] = df['citations'].min()
stats['max'] = df['citations'].max()
stats['avg'] = round(df['citations'].mean(), 2)
stats['med'] = df['citations'].median()

print(f'''statistics for 'citations' column:\nminimum number of citations is {stats['min']}\nmaximum number of citations is {stats['max']}
average number of citations is {stats['avg']}\nmedian number of citations is {stats['med']}''')

# для одного текстового поля посчитайте частоту меток
print({sour : len(df[df['source']==sour]) for sour in df['source'].unique()})