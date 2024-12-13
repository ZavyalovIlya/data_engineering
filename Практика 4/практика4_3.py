import json
import csv
import sqlite3
import pandas as pd

def read_csv(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file,
                                delimiter=';')
        items = list(reader)
    return items

def read_json(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = json.load(file)
    return reader

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(db):
    cur = db.cursor()
    query = '''
            CREATE TABLE IF NOT EXISTs music (
                id integer primary key,
                artist text,
                song text,
                duration_ms integer,
                year integer,
                tempo float,
                genre text,
                explicit bool,
                popularity integer,
                danceability float,
                energy float,
                key integer,
                loudness float)
            '''
    cur.execute(query)

def insert_data(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO music (artist, song, duration_ms, year, tempo, genre,
                              explicit, popularity, danceability, energy, key, loudness)
            VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre,
                              :explicit, :popularity, :danceability, :energy, :key, :loudness)
            '''
    cur.executemany(query, data)
    db.commit()

def select_from_db(db, query):
    cur = db.cursor()
    res = cur.execute(query)
    return [dict(row) for row in res.fetchall()]

def to_json(filename, var):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(var, file, ensure_ascii=False)

db_name = 'outputs/third_task.db'

# connect to db
db = connect_to_db(db_name)

# create music table
create_table(db)

# loading first file
data1 = pd.DataFrame(read_json('inputs/3/_part_1.json')).drop_duplicates()

# loading second file
data2 = pd.DataFrame(read_csv('inputs/3/_part_2.csv')).drop_duplicates()

# Сначала объединим данные по столбцам, информация по которым есть в обоих массивах
df = pd.concat([data1[['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre']],
                data2[['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre']]]).drop_duplicates()

# Затем джоиним столбцы с неполными данными
df2 = df.set_index(['artist','song']).join([
                                      data1[['artist','song','explicit','popularity','danceability']].set_index(['artist','song']),
                                      data2[['artist','song','energy','key','loudness']].set_index(['artist','song'])]
                                         ).drop_duplicates().reset_index()

# Преобразовываем в формат для записи в БД
data = [dict(df2.loc[i]) for i in range(len(df2))]

# Добавляем данные в таблицу
insert_data(db, data)

# вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json
query = '''
SELECT
    artist,
    song,
    duration_ms
FROM music
ORDER BY duration_ms
LIMIT 52
'''
to_json('outputs/third_task1.json', select_from_db(db, query))

# вывод (сумму, мин, макс, среднее) по произвольному числовому полю
query = '''
    SELECT
        SUM(duration_ms) as total_duration,
        ROUND(AVG(duration_ms), 2) as average_duration,
        MIN(duration_ms) as minimal_duration,
        MIN(duration_ms) as maximum_duration
    FROM music
'''
print(*select_from_db(db, query))

# вывод частоты встречаемости для категориального поля
query = '''
        SELECT artist, COUNT(artist) as count
        FROM music
        GROUP BY artist
        ORDER BY count DESC
        LIMIT 10
        '''

select_from_db(db, query)

# вывод первых (VAR+15) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json
query = '''
        SELECT
            song,
            artist
        FROM music
        WHERE explicit = "False"
        ORDER BY popularity DESC
        LIMIT 57
        '''
tmp = select_from_db(db, query)
to_json('outputs/third_task2.json', tmp)

db.close()