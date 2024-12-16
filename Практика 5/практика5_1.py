from pymongo import MongoClient
import pymongo
import json

def read_text(filename):
    with open(filename, 'r', encoding="utf8") as file:
        jobs = file.read().split(sep='=====')

    # парсим текст
    jobs = [
            {x.split('::')[0]: x.split('::')[1]
                 for x in job.split('\n')
                 if len(x.split('::')) > 1} # убираем '' появившиеся при разделении по '\n'
            for job in jobs if len(job) > 1]

    # присвоение типов
    for job in jobs:
        job['salary'] = int(job['salary'])
        job['id'] = int(job['id'])
        job['year'] = int(job['year'])
        job['age'] = int(job['age'])

    return jobs

def connect_db():
    client = MongoClient()
    db = client['practise_5']
    return db.jobs

def to_json(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file, ensure_ascii=False)

# connecting to db
collection = connect_db()

collection.insert_many(read_text('inputs/task_1_item.text'))

# query 1
data = list(collection.find(limit=10).sort({'salary':pymongo.DESCENDING}))
for el in data:
    del el['_id']
to_json('outputs/first_task_q1.json', data)

# query 2
data = list(collection.find(
                            {'age': {'$lt': 30}},
                            limit=15)
                            .sort({'salary':pymongo.DESCENDING}))
for el in data:
    del el['_id']
to_json('outputs/first_task_q2.json', data)

# query 3
data = list(collection.find(
                            {'city': 'Ереван',
                            'job': {'$in': ['Повар', 'Психолог', 'Инженер']}},
                            limit=15)
                            .sort({'age':pymongo.ASCENDING}))
for el in data:
    del el['_id']
to_json('outputs/first_task_q3.json', data)

# query 4
data = collection.count_documents(
                            {'age': {'$gte': 20, '$lte': 50},
                             'year': {'$gte': 2019, '$lte': 2022},
                             '$or': [
                                 {'salary': {'$gte': 50000, '$lte': 75000}},
                                 {'salary': {'$gte': 125000, '$lte': 150000}}]
                            })

to_json('outputs/first_task_q4.json', data)