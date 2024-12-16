'''
Датасет с рождественскими фильмами, взят с kaggle:
https://www.kaggle.com/datasets/jonbown/christmas-movies
'''

from pymongo import MongoClient
import pymongo
import json
import csv
import pickle

def read_csv(filename):
    with open(filename, 'r', encoding="utf8") as file:
        movies = list(csv.DictReader(file, delimiter=';'))

    for movie in movies:
        for el in ['runtime', 'imdb_rating', 'meta_score', 'release_year', 'votes', 'gross']:
            try: movie[el] = float(movie[el])
            except: movie[el] = None

    return movies

def read_json(filename):
    with open(filename, 'r') as file:
        movies = json.load(file)
    return movies

def read_pkl(filename):
    with open(filename, 'rb') as file:
        movies = pickle.load(file)
    return movies

def connect_db():
    client = MongoClient()
    db = client['practise_5']
    return db.movies

def to_json(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)

# connecting to db
collection = connect_db()

# inserting new data into db
collection.insert_many(read_csv('inputs/christmas_movies_1.csv'))
collection.insert_many(read_json('inputs/christmas_movies_2.json'))
collection.insert_many(read_pkl('inputs/christmas_movies_3.pkl'))

# q1_1: 10 первые 10 записей, отсортированных по убыванию по полю runtime
data = list(collection.find(limit=10).sort({'runtime':pymongo.DESCENDING}))
for el in data:
    del el['_id']
to_json('outputs/fourth_task_q1_1.json', data)

# q1_2: первые 7 записей, отфильтрованных по предикату meta_score >= 70, отсортировать по убыванию по полю release_year
data = list(collection.find(
                            {'meta_score': {'$gte': 70}},
                            limit=7)
                            .sort({'release_year':pymongo.DESCENDING}))
for el in data:
    del el['_id']
to_json('outputs/fourth_task_q1_2.json', data)

# q1_3: первые 10 записей по фильтру: type: Movie, genre: Comedy или Adventure или Action, сортировка по убыванию по imdb_rating
data = list(collection.find(
                            {'type': 'Movie',
                            'genre': {'$in': ['Comedy', 'Adventure', 'Action']}},
                            limit=10)
                            .sort({'imdb_rating':pymongo.DESCENDING}))
for el in data:
    del el['_id']
to_json('outputs/fourth_task_q1_3.json', data)

# q1_4: 60 <= runtime <= 100, 5 <= imdb_rating <= 10 or 70 <= meta_score <= 100, genre: Comedy
data = list(collection.find(
                        {'runtime': {'$gte': 60, '$lte': 100},
                         '$or': [
                             {'imdb_rating': {'$gte': 5, '$lte': 10}},
                             {'meta_score': {'$gte': 70, '$lte': 100}}],
                         'genre': 'Comedy',
                        }))

for el in data:
    del el['_id']
to_json('outputs/fourth_task_q1_4.json', data)

# q1_5: кол-во записей жанр Драма, рейтинг PG-13, голосов больше 200000
data = collection.count_documents(
                            {'genre': 'Drama',
                             'rating': 'PG-13',
                             'votes': {'$gt': 200000}
                            })

to_json('outputs/fourth_task_q1_5.json', data)

# q2_1: вывод количества данных по жанрам
query = [{
            '$group': {
                    '_id': '$genre',
                    'count': {'$sum': 1}
            }
            },{
                '$sort': {
                    'count': pymongo.DESCENDING
                }
            }
                ]
res = list(collection.aggregate(query))
to_json('outputs/fourth_task_q2_1.json', res)

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

# q2_2: вывод минимальной, средней, максимальной votes
res = min_max_avg_by(collection, group_what='votes')
to_json('outputs/fourth_task_q2_2.json', res)

# q2_3: вывод минимальной, средней, максимальной imdb_rating по жанру
res = min_max_avg_by(collection, group_by = 'genre', group_what='imdb_rating')
to_json('outputs/fourth_task_q2_3.json', res)

# q2_4: вывод средней imdb_rating по жанру при условии, что votes >= 50000, сортировка по убыванию по avg
query = [
    {
        '$match': {
            'votes': {'$gte': 50000}}
    },
    {
        '$group': {
            '_id': '$genre',
            'avg': {'$avg': '$imdb_rating'}}
    },
    {
        '$sort': {
            'avg': pymongo.DESCENDING
        }
    }
]

res = list(collection.aggregate(query))
to_json('outputs/fourth_task_q2_4.json', res)

# q2_5
query = [
    {
        '$match': {
            'votes': {'$gte': 50000},
            'rating': {'$in': ['PG-13', 'PG', 'G']},
            'release_year': {'$gt': 1999}}
    },
    {
        '$group': {
            '_id': '$director',
            'avg': {'$avg': '$votes'}}
    },
    {
        '$sort': {
            'avg': pymongo.DESCENDING
        }
    }
]

res = list(collection.aggregate(query))
to_json('outputs/fourth_task_q2_5.json', res)

# q3_1: удалить записи, где votes = null
collection.delete_many({
    'votes': None
})

# q3_2: добавить 5 votes всем записям
collection.update_many({},{
        '$inc': {'votes': 5}
    })

# q3_3: увеличить gross всем фильмам комедиям и анимациям вышедшим после 2008 года на 7%
collection.update_many({
    'genre': {'$in': ['Comedy', 'Animation']},
    'release_year': {'$gte': 2008},
    'gross': {'$ne': None}
},{
    '$mul': {'gross': 1.07}
})

# q3_4: для фильмов вышедших в периоды с 1970 по 1980 и с 2004 по 2017 жанров Comedy,Drama увеличить годоса на 2415 и imdb_rating на 8%
collection.update_many({
    '$or':[
        {'release_year': {'$gt': 1970, '$lt': 1980}},
        {'release_year': {'$gt': 2004, '$lt': 2017}}
    ],
    'genre': {'$in': ['Comedy', 'Drama']}
},{
    '$mul': {'imdb_rating': 1.08},
    '$inc': {'votes': 2415}
})

#q3_5: удалить записи с imdb_rating < 5 \\ meta_score < 50 \\ votes < 1000 рейтингов PG-13, PG, G
collection.delete_many({
    '$or': [
        {'imdb_rating': {'$lt': 5}},
        {'meta_score': {'$lt': 50}},
        {'votes': {'$lt': 1000}}
    ],
    'rating': {'$in': ['PG-13', 'PG', 'G']}
})