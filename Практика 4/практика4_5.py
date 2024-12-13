'''
Тематика датасета - Рождественские / новогодние фильмы
Датасет взят с kaggle: https://www.kaggle.com/datasets/jonbown/christmas-movies
Данные состоят из трех файлов:
    * description: файл с основным описанием для каждого фильма, содержит поляЖ
        * title: название фильма
        * rating: возрастное ограничение по американской системе
        * runtime: длительность в минутах
        * genre: жанр
        * release_year: год выпуска
        * description: текстовое описание фильма
        * director: режиссер
        * img_src: постер фильма
        * type: тип (фильм / эпизод ТВ шоу)

    * statistics: файл со статистикой для каждого фильма, состоит из:
        * title: название фильма
        * imdb_rating: рейтинг imdb
        * meta_score: рейтинг по metacritic
        * votes: голоса пользователей
        * gross: кассовые сборы фильма в млн$

    * cast: актеры, снимавшиеся в фильме:
        * actor: имя и фамилия актера
        * movie_title: название фильма
'''

import json
import csv
import sqlite3
import pandas as pd
import msgpack

def read_csv(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file,
                               delimiter=';')
        data = list(reader)

    for film in data:
        try:
            film['runtime'] = int(film['runtime'])
        except:
            film['runtime'] = None
        try:
            film['release_year'] = int(film['release_year'])
        except:
            film['release_year'] = None

    return data

def read_msgpack(filename):
    with open(filename, 'rb') as file:
        data = msgpack.load(file)
    return data

def read_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table_description(db):
    cur = db.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS description(
            id integer primary key,
            movie_title text,
            rating text,
            runtime integer,
            genre text,
            release_year integer,
            description text,
            img_src text,
            type text)
    '''
    cur.execute(query)

def create_table_stats(db):
    cur = db.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS stats(
            movie_title text,
            imdb_rating float,
            meta_score integer,
            votes integer,
            gross float)
    '''
    cur.execute(query)

def create_table_cast(db):
    cur = db.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS cast(
            actor text,
            movie_title text)
    '''
    cur.execute(query)

def insert_into_description(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO description (movie_title, rating, runtime,
                              genre, release_year, description, img_src, type)
            VALUES (:title, :rating, :runtime,
                              :genre, :release_year, :description, :img_src, :type)
            '''
    cur.executemany(query, data)
    db.commit()

def insert_into_stats(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO stats (movie_title, imdb_rating, meta_score,
                              votes, gross)
            VALUES (:title, :imdb_rating, :meta_score,
                              :votes, :gross)
            '''
    cur.executemany(query, data)
    db.commit()

def insert_into_cast(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO cast (actor, movie_title)
            VALUES (:actor, :movie_title)
            '''
    cur.executemany(query, data)
    db.commit()

def update_stats(db, updates):
    for film in updates:
        cur = db.cursor()
        cur.execute('''UPDATE stats
                           SET imdb_rating = ?,
                               meta_score = ?,
                               votes = ?,
                               gross = ?
                           WHERE movie_title = ?''',
                   list(film.values()))
        db.commit()

def select_from_db(db, query):
    cur = db.cursor()
    res = cur.execute(query)
    return [dict(row) for row in res.fetchall()]

def to_json(file_path, var):
    # first printing data
    for el in var:
        print(*el.values())
    # then exporting to json
    with open(file_path, 'w') as file:
        json.dump(var, file)

# import data from files
movies_description = read_csv('inputs/5/movies_description.csv')
movies_stats = read_json('inputs/5/movies_statistics.json')
movies_cast = read_msgpack('inputs/5/movies_cast.msgpack')

# connect to db
db = connect_to_db('outputs/fifth_task.db')

create_table_description(db)
create_table_stats(db)
create_table_cast(db)

# insert data into db tables
insert_into_description(db, movies_description)
insert_into_stats(db, movies_stats)
insert_into_cast(db, movies_cast)

# query 1: updating film statistics with new values
stats_updates = read_json('inputs/5/movies_statistics_update.json')
update_stats(db, stats_updates)

# query2: select 7 films that are comedy, for general audience, the newest
query = '''
        SELECT movie_title from description
        WHERE genre LIKE "%comedy%"
        AND rating = "G"
        ORDER BY release_year DESC
        LIMIT 7
        '''

to_json('outputs/fifth_task_q2.json', select_from_db(db, query))

# query 3: select 5 best actors by the average imdb_rating of the films they starred in
query = '''
        SELECT
        	actor,
        	round(avg(imdb_rating) ,1) as imdb_score
        FROM cast c
        JOIN stats s ON c.movie_title = s.movie_title
        GROUP BY actor
        ORDER BY imdb_score DESC
        LIMIT 5
        '''
to_json('outputs/fifth_task_q3.json', select_from_db(db, query))

# query 4: select 5 actors that starred in most films
query = '''
        SELECT
            actor,
            count(movie_title) as n_films
        FROM cast
        GROUP BY actor
        ORDER BY n_films DESC
        LIMIT 5
        '''

to_json('outputs/fifth_task_q4.json', select_from_db(db, query))

# query 5: select 5 richest actors (sum of profits (in million $) of the films where actor starred in)
query = '''
        SELECT
        	c.actor,
        	sum(s.gross) as sum_gross
        FROM cast c
        JOIN stats s ON c.movie_title = s.movie_title
        GROUP BY actor
        ORDER BY sum_gross DESC
        LIMIT 5
        '''
to_json('outputs/fifth_task_q5.json', select_from_db(db, query))

# query 6: film ratings statistic
query = '''
        SELECT
        	CASE WHEN rating = "" THEN "Not specified" ELSE rating END as rating,
        	count(rating) as number
        FROM description
        GROUP BY rating
        ORDER BY number DESC
        '''
to_json('outputs/fifth_task_q6.json', select_from_db(db, query))

# query 7: statistics of film votes
query = '''
        SELECT
        	min(votes) as minimum,
        	max(votes) as maximum,
        	round(avg(votes),2) as mean,
        	sum(votes) as sum
        FROM stats
        '''
to_json('outputs/fifth_task_q7.json', select_from_db(db, query))

db.close()