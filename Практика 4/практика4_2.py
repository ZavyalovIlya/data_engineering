import sqlite3
import msgpack

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table_sub(db):
    cur = db.cursor()
    query = '''
            CREATE TABLE IF NOT EXISTs reviews (
                id integer primary key,
                name text references flats(name),
                rating float,
                convenience integer,
                security integer,
                functionality integer,
                comment text)
            '''
    cur.execute(query)

def insert_into_db_sub(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO reviews (name, rating, convenience, security, functionality, comment)
            VALUES (:name, :rating, :convenience, :security, :functionality, :comment)
            '''
    cur.executemany(query, data)
    db.commit()

def select_from_db(db, query):
    cur = db.cursor()
    res = cur.execute(query)
    return [dict(row) for row in res.fetchall()]

# read data file
with open('inputs/1-2/subitem.msgpack', 'rb') as file:
    sub_data = msgpack.load(file)

db_name = 'outputs/first_task1.db'

# connect to db
db = connect_to_db(db_name)

# create table if not exists
create_table_sub(db)

# insert data into db
insert_into_db_sub(db, sub_data)

# query 1 - рейтинг улиц исходя из среднего рейтинга объектов, расположенных на этой улице
query = '''
        SELECT
        	f.street,
        	round(avg(r.rating), 2) as avg_rating
        FROM reviews r
        JOIN flats f ON r.name = f.name
        GROUP BY street
        ORDER BY avg_rating DESC
'''
for line in select_from_db(db, query):
    print(line)

# query 2 - топ-5 наименее безопасных городов
query = '''
        SELECT
        	ROUND(AVG(r.security),2) as security,
        	f.city
        FROM reviews r
        JOIN flats f ON r.name = f.name
        GROUP BY city
        ORDER BY security
        LIMIT 5
'''
for line in select_from_db(db, query):
    print(line)

# query 3 - наиболее комментируемый объект в каждом городе
query = '''
        SELECT
        	f.city,
        	c.name,
        	MAX(c.count) as comments
        FROM
        (
        	SELECT
        		name,
        		COUNT(name) as count
        	FROM reviews
        	GROUP BY name
        ) c

        JOIN flats f
        ON c.name = f.name
        GROUP BY f.city
'''
for line in select_from_db(db, query):
    print(line)

db.close()