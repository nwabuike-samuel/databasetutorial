# pylint: disable=missing-docstring, trailing-whitespace, line-too-long, invalid-name
################################## Part1 ###################################
import csv
import sqlite3
from sqlite3 import Error

#### Task 1
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
    # This function creates a table called CarSharing and imports the data from the csv file
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS CarSharing (id INTEGER PRIMARY KEY, timestamps TEXT, season TEXT, holiday TEXT, workingday TEXT, weather TEXT, temp REAL, temp_feel REAL, humidity REAL, windspeed REAL, demand REAL)''')
    
    with open('CarSharing.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute('''INSERT INTO CarSharing (id, timestamps, season, holiday, workingday, weather, temp, temp_feel, humidity, windspeed, demand) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row)

def create_backup(conn):
    # This function creates two backup tables, Backup1 and Backup2, and populates them with half of the data from the CarSharing table
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS Backup1 (id INTEGER PRIMARY KEY, timestamps TEXT, season TEXT, weather TEXT, temp REAL,humidity REAL)''')

    cursor.execute('''INSERT INTO Backup1 SELECT id, timestamps, season, weather, temp, humidity FROM CarSharing ORDER BY RANDOM() LIMIT(SELECT COUNT(*)/2 FROM CarSharing)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Backup2 (id INTEGER PRIMARY KEY, holiday TEXT, workingday TEXT, temp_feel REAL, windspeed REAL, demand REAL)''')

    cursor.execute('''INSERT INTO Backup2 SELECT id, holiday, workingday, temp_feel, windspeed, demand FROM CarSharing ORDER BY RANDOM() LIMIT (SELECT COUNT(*)/2 FROM CarSharing)''')

    conn.commit()
    cursor.close()

#### Task 2

def create_humidity_column(conn):
    # This function creates a new column called humidity_category and populates it with the appropriate category based on the humidity value
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE CarSharing ADD COLUMN humidity_category text;")

    query = """UPDATE CarSharing SET humidity_category = CASE WHEN "humidity" < 55 THEN "Dry" WHEN "humidity" > 55 AND "humidity" < 65 THEN "Sticky" ELSE "Oppressive" END;"""
    
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    conn.commit()
    cursor.close()


#### Task 3
def create_weather_table(conn):
    # This function creates a new table called Weather and populates it with the data from the CarSharing table and drops the weather, temp, temp_feel, humidity, windspeed, humidity_category columns from the CarSharing table
    cursor = conn.cursor()
    
    query1 = """CREATE TABLE IF NOT EXISTS Weather AS SELECT id, weather, temp, temp_feel, humidity, windspeed, humidity_category FROM CarSharing;"""
    query2 = """PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS CarSharing_NewTable(id INTEGER PRIMARY KEY,
    timestamps TEXT,
    season TEXT,
    holiday TEXT,
    workingday TEXT,
    demand REAL);
    INSERT INTO CarSharing_NewTable(id, timestamps, season, holiday, workingday, demand) SELECT id, timestamps, season, holiday, workingday, demand FROM CarSharing;
    DROP TABLE CarSharing;
    ALTER TABLE CarSharing_NewTable RENAME TO CarSharing;
    COMMIT;
    PRAGMA foreign_keys=on;"""

    cursor.execute(query1)
    cursor.executescript(query2)
    conn.commit()

def add_workingday_code(conn):
    # This function creates a new column called workingday_code and populates it with the appropriate code based on the workingday value (1 = Yes, 0 = No)
    query1 = """SELECT DISTINCT workingday FROM CarSharing;"""
    query2 = """ALTER TABLE CarSharing ADD COLUMN workingday_code text;
    UPDATE CarSharing SET workingday_code = CASE workingday WHEN "No" THEN "0" WHEN "Yes" THEN "1" ELSE "" END;"""

    cursor = conn.cursor()
    cursor.execute(query1)
    cursor.executescript(query2)
    conn.commit()

def add_holiday_code(conn):
    # This function creates a new column called holiday_code and populates it with the appropriate code based on the holiday value (1 = Yes, 0 = No)
    query1 = """SELECT DISTINCT holiday FROM CarSharing;"""
    query2 = """ALTER TABLE CarSharing ADD COLUMN holiday_code text;
    UPDATE CarSharing SET holiday_code = CASE holiday WHEN "No" THEN "0" WHEN "Yes" THEN "1" ELSE "" END;"""

    cursor = conn.cursor()
    cursor.execute(query1)
    cursor.executescript(query2)
    conn.commit()

def create_holiday_table(conn):
    # This function creates a new table called Holiday and populates it with the data from the CarSharing table and drops the holiday, workingday_code, holiday_code columns from the CarSharing table
    cursor = conn.cursor()
    query1 = """CREATE TABLE IF NOT EXISTS holiday AS SELECT id, holiday, workingday, workingday_code, holiday_code FROM CarSharing;"""
    query2 = """PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS CarSharing_NewTable(id INTEGER PRIMARY KEY,
    timestamps TEXT,
    season TEXT,
    demand REAL);
    INSERT INTO CarSharing_NewTable(id, timestamps, season, demand) SELECT id, timestamps, season, demand FROM CarSharing;
    DROP TABLE CarSharing;
    ALTER TABLE CarSharing_NewTable RENAME TO CarSharing;
    COMMIT;
    PRAGMA foreign_keys=on;"""

    cursor.execute(query1)
    cursor.executescript(query2)
    conn.commit()

def create_time_table(conn):
    # This function creates a new table called Time and populates it with modified data from the CarSharing table and drops the timestamps and season columns from the CarSharing table
    cursor = conn.cursor()
    query1 = """CREATE TABLE IF NOT EXISTS Time
                    (id INTEGER PRIMARY KEY,
                    timestamps TEXT,
                    hour INTEGER,
                    weekday_name TEXT,
                    month TEXT,
                    season TEXT)"""
    query2 = """INSERT INTO Time (id, timestamps, hour, weekday_name, month, season)
                    SELECT id, timestamps, strftime('%H', timestamps) AS hour,
                    strftime('%W', timestamps, 'weekday 1') AS weekday_name,
                    strftime('%m', timestamps, 'start of month') AS month,
                    season
                    FROM CarSharing"""
    query3 = """PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS CarSharing_NewTable(id INTEGER PRIMARY KEY,
    demand REAL);
    INSERT INTO CarSharing_NewTable(id, demand) SELECT id, demand FROM CarSharing;
    DROP TABLE CarSharing;
    ALTER TABLE CarSharing_NewTable RENAME TO CarSharing;
    COMMIT;
    PRAGMA foreign_keys=on;"""
    cursor.execute(query1)
    cursor.executescript(query2)
    cursor.executescript(query3)
    conn.commit()

#### Task 4
def question4a(conn):
    # The date and time (timestamp) when we had the lowest temperature and the corresponding demand rate
    cursor = conn.cursor()

    query = """SELECT timestamps, demand FROM CarSharing c JOIN Time t ON c.id = t.id JOIN Weather w ON c.id = w.id WHERE w.temp = (SELECT MIN(temp) FROM Weather)"""

    query_result = cursor.execute(query)
    results = query_result.fetchall()

    for row in results:
        print(f"Timestamp: {row[0]}")
        print(f"Demand: {row[1]}")

    conn.commit()
    cursor.close()

def question4b(conn):
    # The average, highest, and lowest windspeed and humidity for working days (i. e., workingday=“Yes”) and non-working days ((i. e., workingday=“No”) in 2017 and the corresponding windspeed and humidity values.
    cursor = conn.cursor()
    query = """SELECT workingday, AVG(windspeed) AS avg_windspeed, MAX(windspeed) AS max_windspeed, MIN(windspeed) AS min_windspeed, AVG(humidity) AS avg_humidity, MAX(humidity) AS max_humidity, MIN(humidity) AS min_humidity FROM CarSharing c JOIN Time t ON c.id = t.id JOIN Holiday h ON c.id = h.id JOIN Weather w ON c.id = w.id WHERE strftime('%Y', t.timestamps) = '2017' GROUP BY h.workingday"""

    query_result = cursor.execute(query)
    results = query_result.fetchall()

    for row in results:
        print(f"Workingday: {row[0]}")
        print(f"Average Windspeed: {row[1]}")
        print(f"Max Windspeed: {row[2]}")
        print(f"Min Windspeed: {row[3]}")
        print(f"Average Humidity: {row[4]}")
        print(f"Max Humidity: {row[5]}")
        print(f"Min Humidity: {row[6]}")
        print("\n")

    conn.commit()
    cursor.close()

def question4c(conn):
    #The weekday, month, and season when we had the highest average demand rates throughout 2017 and the corresponding average demand rates
    cursor = conn.cursor()
    query = """SELECT t.weekday_name, t.month, t.season, AVG(c.demand) AS avg_demand FROM CarSharing c JOIN Time t ON c.id = t.id WHERE strftime('%Y', t.timestamps) = '2017' GROUP BY t.weekday_name, t.month, t.season ORDER BY avg_demand DESC LIMIT 1"""
    
    query_result = cursor.execute(query)
    result = query_result.fetchone()

    print("Highest Average Demand Rates in 2017:")
    print(f"Weekday: {result[0]}")
    print(f"Month: {result[1]}")
    print(f"Season: {result[2]}")
    print(f"Average Demand: {result[3]}")

    conn.commit()
    cursor.close()

def question4d(conn):
    # The average demand rates for each Dry, Sticky, and Oppressive humidity in 2017 sorted in descending order based on their average demand rates
    cursor = conn.cursor()
    query = """SELECT w.humidity, w.humidity_category, AVG(c.demand) AS avg_demand FROM CarSharing c JOIN Weather w ON c.id = w.id JOIN Time t ON c.id = t.id WHERE strftime('%Y', t.timestamps) = '2017' GROUP BY w.humidity_category ORDER BY avg_demand DESC"""
    query_result = cursor.execute(query)
    results = query_result.fetchall()

    print("Average Demand Rates for Each Humidity Level in 2017:")
    for result in results:
        print(f"Humidity: {result[0]}, Category: {result[1]}, Average Demand: {result[2]}")

    conn.commit()
    cursor.close()


def main():
    database = r"cars.db"
    #### Task 1
    # create a database connection
    conn = create_sql_conn(database)

    with conn:
        #### Task 1
        print("Question 1")
        create_carsharing_table(conn)
        print("CarSharing Table has been created successfully.")
        create_backup(conn)
        print("Backups created.")
        print("\nQuestion 2")
        #### Task 2
        create_humidity_column(conn)
        print("Humidity column has been added to the CarSharing table.")
        #### Task 3
        print("\nQuestion 3a")
        create_weather_table(conn)
        print("Weather table has been created successfully.")
        print("\nQuestion 3b")
        add_workingday_code(conn)
        print("Workingday code column has been added to the Weather table.")
        add_holiday_code(conn)
        print("Holiday code column has been added to the Weather table.")
        print("\nQuestion 3c")
        create_holiday_table(conn)
        print("Holiday table has been created successfully.")
        create_time_table(conn)
        print("Time table has been created successfully.")
        #### Task 4
        print("\nQuestion 4a")
        question4a(conn)
        print("\nQuestion 4b")
        question4b(conn)
        print("\nQuestion 4c")
        question4c(conn)
        print("\nQuestion 4d")
        question4d(conn)
    
if __name__ == '__main__':
    main()
    