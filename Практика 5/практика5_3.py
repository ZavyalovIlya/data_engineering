from pymongo import MongoClient
import pymongo
import json
import pickle

def read_pkl(filename):
    with open(filename, 'rb') as file:
        data = pickle.load(file)
    return data

def connect_db():
    client = MongoClient()
    db = client['practise_5']
    return db.jobs

# connecting to db
collection = connect_db()

# inserting new data into db
collection.insert_many(read_pkl('inputs/task_3_item.pkl'))

def delete_by_salary(collection):
    return collection.delete_many({
        '$or': [
            {'salary': {'$lt': 25000}},
            {'salary': {'$gt': 175000}}
        ]
    })

# удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
delete_by_salary(collection)

def inc_age(collection):
    return collection.update_many({},{
        '$inc': {'age': 1}
    })

# увеличить возраст (age) всех документов на 1
inc_age(collection)

def inc_salary_by_job(collection, list_jobs):
    return collection.update_many({
        'job': {'$in': list_jobs}
    },{
        '$mul': {'salary': 1.05}
    })

# поднять заработную плату на 5% для произвольно выбранных профессий
inc_salary_by_job(collection, ['Менеджер', 'Инженер'])

def inc_salary_by_city(collection, list_city):
    return collection.update_many({
        'city': {'$in': list_city}
    },{
        '$mul': {'salary': 1.07}
    })

# поднять заработную плату на 7% для произвольно выбранных городов
inc_salary_by_city(collection, ['Куэнка', 'Ереван'])

def inc_salary(collection):
    return collection.update_many({
        'city': {'$in': ['Куэнка', 'Ереван']},
        'job': {'$in': ['Медсестра', 'Менеджер']},
        'age': {'$gte': 19, '$lte': 46}
    },{
        '$mul': {'salary': 1.1}
    })

# поднять заработную плату на 10% для выборки по сложному предикату (город, проф, возр)
inc_salary(collection)

def delete_by_job_city(collection):
    return collection.delete_many({
        'city': {'$in': ['Куэнка', 'Ереван']},
        'job': {'$in': ['Медсестра', 'Менеджер']}
    })

# удалить из коллекции записи по произвольному предикату
delete_by_job_city(collection)