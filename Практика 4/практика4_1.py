import sqlite3
import msgpack
import json

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(db):
    cur = db.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTs flats(
            id integer primary key,
            name text,
            street text,
            city text,
            zipcode integer,
            floors integer,
            year integer,
            parking bool,
            prob_price integer,
            views integer)
    '''
    cur.execute(query)

def insert_into_db(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO flats (id, name, street, city, zipcode,
                              floors, year, parking, prob_price, views)
            VALUES (:id, :name, :street, :city, :zipcode,
                              :floors, :year, :parking, :prob_price, :views)
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

# Задание 0

# read data file
with open('inputs/1-2/item.msgpack', 'rb') as file:
    data = msgpack.load(file)

db_name = 'outputs/first_task1.db'

# connect to db
db = connect_to_db(db_name)

# create table if not exists
create_table(db)

# insert data into db
insert_into_db(db, data)

# вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json

query = 'SELECT * FROM flats ORDER BY year LIMIT 52'
tmp = select_from_db(db, query)
to_json('outputs/first_task1.json', tmp)

# вывод (сумму, мин, макс, среднее) по произвольному числовому полю

query = '''
    SELECT
        SUM(views) as total_views,
        ROUND(AVG(views), 2) as average_views,
        MIN(views) as minimal_views,
        MIN(views) as maximum_views
    FROM flats
'''
print(*select_from_db(db, query))

# вывод частоты встречаемости для категориального поля

query = '''
        SELECT city, COUNT(city) as count
        FROM flats
        GROUP BY city
        '''

for city in select_from_db(db, query):
    print(f"{city['city']}: {city['count']}")

# вывод первых (VAR+10) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json.

query = '''
        SELECT *
        FROM flats
        WHERE city = "Луго"
        ORDER BY views DESC
        LIMIT 52
        '''
tmp = select_from_db(db, query)
to_json('outputs/first_task2.json', tmp)

db.close()