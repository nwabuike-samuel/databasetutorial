import csv
import sqlite3
from sqlite3 import Error


def create_sql_conn(db_file):
    """ Create a connection to the SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def create_carsharing_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS CarSharing (id INTEGER PRIMARY KEY,
    timestamps TEXT,
    season TEXT,
    holiday TEXT,
    workingday TEXT,
    weather TEXT,
    temp REAL,
    temp_feel REAL,
    humidity REAL,
    windspeed REAL,
    demand REAL)''')
    
    with open('CarSharing.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute('''INSERT INTO CarSharing (id, timestamps, season, holiday, workingday, weather, temp, temp_feel, humidity, windspeed, demand) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row)

def create_backup(conn):
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS Backup1 (id INTEGER PRIMARY KEY,
    timestamps TEXT,
    season TEXT,
    weather TEXT,
    temp REAL,
    humidity REAL)''')

    cursor.execute('''INSERT INTO Backup1 SELECT timestamps, season, weather, temp, humidity FROM CarSharing ORDER BY RANDOM() LIMIT (SELECT COUNT(*)/2 FROM CarSharing)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Backup2 (id INTEGER PRIMARY KEY,
    holiday TEXT,
    workingday TEXT,
    temp_feel REAL,
    windspeed REAL,
    demand REAL)''')

    cursor.execute('''INSERT INTO Backup2 SELECT holiday, workingday, temp_feel, windspeed, demand FROM CarSharing ORDER BY RANDOM() LIMIT (SELECT COUNT(*)/2 FROM CarSharing)''')

    conn.commit()
    cursor.close()

def create_humidity_column(conn):
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE CarSharing ADD COLUMN temp_category text;")

    query = """UPDATE CarSharing SET humidity_category = CASE WHEN "humidity" < 55 THEN "Dry" WHEN "humidity" > 55 AND "humidity" < 65 THEN "Sticky" WHEN "humidity" > 65 THEN "Oppressive" ELSE "" END;"""
    
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        print(row)

def create_weather_table(conn):
    cursor = conn.cursor()
    
    query1 = """CREATE TABLE Weather AS SELECT id, weather, temp, temp_feel, humidity, windspeed, humidity_category FROM CarSharing;"""
    query2 = """PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS CarSharing_NewTable(id INTEGER PRIMARY KEY,
    timestamps TEXT,
    season TEXT,
    holiday TEXT,
    workingday TEXT,
    weather TEXT,
    temp REAL,
    temp_feel REAL,
    humidity REAL,
    windspeed REAL,
    demand REAL,
    humidity_category TEXT);
    INSERT INTO CarSharing_NewTable(id, timestamps, season, holiday, workingday, weather, temp, temp_feel, humidity, windspeed, demand, humidity_category) SELECT id, timestamps, season, holiday, workingday, weather, temp, temp_feel, humidity, windspeed, demand, humidity_category FROM CarSharing;
    DROP TABLE CarSharing;
    ALTER TABLE CarSharing_NewTable RENAME TO CarSharing;
    COMMIT;
    PRAGMA foreign_keys=on;"""

    cursor.execute(query1)
    cursor.executescript(query2)
    conn.commit()
    return cursor.lastrowid

# def add_workingday_code():

# def add_holiday_code():

# def create_holiday_table():

# def create_time_table():
#     query = """PRAGMA foreign_keys=off;
#     BEGIN TRANSACTION;
#     ALTER TABLE time ADD COLUMN "hour" text;
#     ALTER TABLE time ADD COLUMN "weekday_name" text;
#     ALTER TABLE time ADD COLUMN "month_name" text;
#     UPDATE time SET hour = strftime('%H', datetime(timestamp));
#     UPDATE time SET weekday_name = strftime('%w', datetime(timestamp));
#     UPDATE time SET month_name = strftime('%m', datetime(timestamp));
#     COMMIT;
#     PRAGMA foreign_keys=on;"""

#     cur = conn.cursor()
#     cur.execute("CREATE TABLE time AS SELECT timestamp FROM CarSharing;")
#     cur.executescript(query)
#     conn.commit()
#     return cur.lastrowid


def main():
    database = r"D:\Programming\OmoT\cars.db"

    conn = create_sql_conn(database)