import pymongo
from pymongo import MongoClient
import csv
import json

def read_csv(filename):
    with open(filename, 'r', encoding="utf8") as file:
        jobs2 = list(csv.DictReader(file, delimiter=';'))

    for job in jobs2:
        job['salary'] = int(job['salary'])
        job['id'] = int(job['id'])
        job['year'] = int(job['year'])
        job['age'] = int(job['age'])

    return jobs2

def connect_db():
    client = MongoClient()
    db = client['practise_5']
    return db.jobs

def to_json(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file, ensure_ascii=False)

# connecting to db
collection = connect_db()

# inserting new data into db
collection.insert_many(read_csv('inputs/task_2_item.csv'))

def min_max_avg_by(collection, group_by=' ', group_what=' '):

    query = [{
        '$group': {
            '_id': '$'+group_by,
            'max': {'$max': '$'+group_what},
            'min': {'$min': '$'+group_what},
            'avg': {'$avg': '$'+group_what}
        }
    }]

    return list(collection.aggregate(query))

def count_by(collection, group_by=' '):
    query = [{
        '$group': {
                '_id': '$'+group_by,
                'count': {'$sum': 1}}
    }]

    return list(collection.aggregate(query))

# вывод минимальной, средней, максимальной salary
res = min_max_avg_by(collection, group_what='salary')
to_json('outputs/second_task_q1.json', res)

# вывод количества данных по представленным профессиям
res = count_by(collection, group_by='job')
to_json('outputs/second_task_q2.json', res)

# вывод минимальной, средней, максимальной salary по городу
res = min_max_avg_by(collection, group_by='city', group_what='salary')
to_json('outputs/second_task_q3.json', res)

# вывод минимальной, средней, максимальной salary по профессии
res = min_max_avg_by(collection, group_by='job', group_what='salary')
to_json('outputs/second_task_q4.json', res)

# вывод минимального, среднего, максимального возраста по городу
res = min_max_avg_by(collection, group_by='city', group_what='age')
to_json('outputs/second_task_q5.json', res)

# вывод минимального, среднего, максимального возраста по профессии
res = min_max_avg_by(collection, group_by='job', group_what='age')
to_json('outputs/second_task_q6.json', res)

def get_max_by_min(collection, max, min):
    res = list(collection.find(limit=1).sort({min:pymongo.ASCENDING, max:pymongo.DESCENDING}))
    for el in res:
        del el['_id']
    return res

def get_min_by_max(collection, max, min):
    res = list(collection.find(limit=1).sort({max:pymongo.DESCENDING, min:pymongo.ASCENDING}))
    for el in res:
        del el['_id']
    return res

# вывод максимальной заработной платы при минимальном возрасте
res = get_max_by_min(collection, max='salary', min='age')
to_json('outputs/second_task_q7.json', res)

# вывод минимальной заработной платы при максимальной возрасте
res = get_min_by_max(collection, max='age', min='salary')
to_json('outputs/second_task_q7.json', res)

# вывод минимального, среднего, максимального возраста по городу,
# при условии, что заработная плата больше 50 000,
# отсортировать вывод по убыванию по полю avg
query = [
    {
        '$match': {
            'salary': {'$gt': 50000}}
    },
    {
        '$group': {
            '_id': '$city',
            'max': {'$max': '$age'},
            'min': {'$min': '$age'},
            'avg': {'$avg': '$age'}}
    },
    {
        '$sort': {
            'avg': pymongo.DESCENDING
        }
    }
]

res = list(collection.aggregate(query))
to_json('outputs/second_task_q8.json', res)

# вывод минимальной, средней, максимальной salary
# в произвольно заданных диапазонах по городу, профессии,
# и возрасту: 18<age<25 & 50<age<65 ???? заменено на 18<age<25 || 50<age<65

query = [
    {
        '$match': {
            'city': {'$in': ['Сьюдад-Реаль', 'Ереван', 'Рига', 'Санхенхо']},
            'job': {'$in': ['Инженер', 'Менеджер']},
            '$or': [
                {'age': {'$gt': 18, '$lt': 25}},
                {'age': {'$gt': 50, '$lt': 65}}
            ]
        }
    },
    {
        '$group': {
            '_id': 'result',
            'max': {'$max': '$salary'},
            'min': {'$min': '$salary'},
            'avg': {'$avg': '$salary'}}
   }
]
res = list(collection.aggregate(query))
to_json('outputs/second_task_q9.json', res)

# сколько получают IT-специалисты в среднем в разных городах
query = [
    {
        '$match': {
            'job': 'IT-специалист'}
    },
    {
        '$group': {
            '_id': '$city',
            'avg': {'$avg': '$salary'}}
    },
    {
        '$sort': {
            'avg': pymongo.DESCENDING
        }
    }
]
res = list(collection.aggregate(query))
to_json('outputs/second_task_q10.json', res)