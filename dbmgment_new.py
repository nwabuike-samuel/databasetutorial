import csv
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def create_CarSharing_table(conn):
    # This function creates a table called CarSharing.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE IF NOT EXISTS CarSharing ( id text PRIMARY KEY,    timestamp text, season text, holiday text, workingday text, weather text, temp real, temp_feel real, humidity real, windspeed real, demand real);"""
    # """CREATE TABLE IF NOT EXISTS CarSharing ( id integer PRIMARY KEY,    timestamp text NOT NULL, season text NOT NULL, holiday text NOT NULL, workingday text NOT NULL, weather text NOT NULL, temp real NOT NULL, temp_feel real NOT NULL, humidity real NOT NULL, windspeed real NOT NULL, demand real NOT NULL);"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_CarSharingBackup_table(conn):
    # This function creates backup for the CarSharing table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """ CREATE TABLE CarSharingBackup AS SELECT * FROM CarSharing;"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def import_csv_to_table(conn):
    # This function imports the data from a csv file and inputs them into the CarSharing table
    cur = conn.cursor()
    data = open('D:\Programming\OmoT\CarSharing_without_header.csv', 'r')
    contents = csv.reader(data)
    insert_records = "INSERT INTO CarSharing (id, timestamp, season, holiday, workingday, weather, temp, temp_feel, humidity, windspeed, demand) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cur.executemany(insert_records, contents)

    conn.commit()

    print("data imported successfully")


def select_all(conn, query):
    # This function outputs the first 20 records in the table
    cur = conn.cursor()
    # cur.execute("SELECT * FROM CarSharing LIMIT 20")
    # cur.execute("SELECT * FROM temperature LIMIT 20")
    # cur.execute("SELECT * FROM weather LIMIT 20")
    cur.execute(query)
    # data = cur.execute("SELECT * FROM time LIMIT 20")

    # for column in data.description:
    #     print(column[0])

    rows = cur.fetchall()
    for row in rows:
        print(row)

    # cur.close()


def add_column(conn):
    # This function answers question 2
    # It creates adds a column "temp_category" to the CarSharing table

    query = """UPDATE CarSharing SET temp_category = CASE WHEN "temp_feel" < 10 THEN "Cold" WHEN "temp_feel" > 10 AND "temp_feel" < 25 THEN "Mild" WHEN "temp_feel" > 25 THEN "Hot" ELSE "" END;"""
    cur = conn.cursor()
    cur.execute("ALTER TABLE CarSharing ADD COLUMN temp_category text;")
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print(row)


def create_temp_table(conn):
    # This function answers question 3
    # It creates a table named "temerature"
    # It drops  column "temp" and "temp_feel" from CarSharing table

    query1 = """CREATE TABLE temperature AS SELECT temp, temp_feel, temp_category FROM CarSharing;"""
    query2 = """PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS CarSharing_NewTable( id text PRIMARY KEY, timestamp text, season text, holiday text, workingday text, weather text, humidity real, windspeed real, demand real, temp_category text);
    INSERT INTO CarSharing_NewTable(id, timestamp, season, holiday, workingday, weather, humidity, windspeed, demand, temp_category) SELECT id, timestamp, season, holiday, workingday, weather, humidity, windspeed, demand, temp_category FROM CarSharing;
    DROP TABLE CarSharing;
    ALTER TABLE CarSharing_NewTable RENAME TO CarSharing;
    COMMIT;
    PRAGMA foreign_keys=on;"""

    cur = conn.cursor()
    cur.execute(query1)
    cur.executescript(query2)
    conn.commit()
    return cur.lastrowid


def add_weathercode(conn):
    # This function answers question 4
    # It ifentifies the distinct values in the weather column
    # It adds a column "weather_code" to the CarSharing table and assign a number based on the distinct values

    query = """ALTER TABLE CarSharing ADD COLUMN weather_code text;
    UPDATE CarSharing SET weather_code = CASE weather WHEN "Clear or partly cloudy" THEN "1" WHEN "Mist" THEN "2" WHEN "Light snow or rain" THEN "3" WHEN "heavy rain/ice pellets/snow + fog" THEN "4" ELSE "" END;"""

    cur = conn.cursor()
    cur.execute("SELECT DISTINCT weather FROM CarSharing;")
    cur.executescript(query)
    conn.commit()
    return cur.lastrowid


def create_weather_table(conn):
    # This function answers question 5
    # It creates a table called "Weather" and copies the "Weather" and "weather_code" from CarSharing table
    # It drops the weather column from the CarSharing table

    query = """ PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS CarSharing_NewTable( id text PRIMARY KEY, timestamp text, season text, holiday text, workingday text, humidity real, windspeed real, demand real, temp_category text, weather_code text);
    INSERT INTO CarSharing_NewTable(id, timestamp, season, holiday, workingday, humidity, windspeed, demand, temp_category, weather_code) SELECT id, timestamp, season, holiday, workingday, humidity, windspeed, demand, temp_category, weather_code FROM CarSharing;
    DROP TABLE CarSharing;
    ALTER TABLE CarSharing_NewTable RENAME TO CarSharing;
    COMMIT;
    PRAGMA foreign_keys=on;"""

    cur = conn.cursor()
    cur.execute("CREATE TABLE weather AS SELECT weather, weather_code FROM CarSharing;")
    cur.executescript(query)
    conn.commit()
    return cur.lastrowid


def create_time_table(conn):
    # This function answers question 6
    # It creates a table called "time"
    # Each row has the columsn "timestamp", "hour", "weekday name" and "month name"

    query = """PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    ALTER TABLE time ADD COLUMN "hour" text;
    ALTER TABLE time ADD COLUMN "weekday_name" text;
    ALTER TABLE time ADD COLUMN "month_name" text;
    UPDATE time SET hour = strftime('%H', datetime(timestamp));
    UPDATE time SET weekday_name = strftime('%w', datetime(timestamp));
    UPDATE time SET month_name = strftime('%m', datetime(timestamp));
    COMMIT;
    PRAGMA foreign_keys=on;"""

    cur = conn.cursor()
    cur.execute("CREATE TABLE time AS SELECT timestamp FROM CarSharing;")
    cur.executescript(query)
    conn.commit()
    return cur.lastrowid

def create_car_average_demand_table(conn):
    #This function is part of our solution for qustion 7b
    # This function creates Car average demand table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE CarAverageDemand as SELECT CASE strftime('%w', timestamp) WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday' 
        END as weekday_name,
        CASE strftime('%m', timestamp)
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
            END AS month,
            season,
            AVG(demand) AS avg_demand
        FROM CarSharing
        WHERE strftime('%Y', timestamp) = '2017'
        GROUP BY weekday_name, month, season
        ORDER BY avg_demand DESC
        """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_highestlowestdemand_table(conn):
    #This function answers question 7b
    # This function creates a highest and lowest demand table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = '''CREATE TABLE HighestLowestDemand as SELECT weekday_name, month, season,avg_demand
            FROM CarAverageDemand
            WHERE avg_demand = (SELECT max(avg_demand) from CarAverageDemand)
            or avg_demand = (SELECT min(avg_demand) from CarAverageDemand)'''
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_hourly_highest_average_demand_table(conn):
    #This function is part of the solution for question 7c
    # This function creates a hourly highest average demand table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE HourlyHighestAverageDemand AS SELECT strftime('%H', timestamp) AS hour, CASE strftime('%w', timestamp) WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday' 
        END as weekday_name,
    AVG(demand) AS avg_demand
    FROM CarSharing
    WHERE strftime('%Y', timestamp) = '2017'
    and weekday_name = (SELECT weekday_name from HighestLowestDemand WHERE avg_demand = (SELECT max(avg_demand) from HighestLowestDemand))
    GROUP BY hour, weekday_name
    ORDER BY avg_demand DESC"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)



def create_hourly_lowest_average_demand_table(conn):
    # This function is part of the solution for question 7c
    # This function creates a hourly lowest average demand table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE HourlyLowestAverageDemand AS SELECT strftime('%H', timestamp) AS hour, CASE strftime('%w', timestamp) WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday' 
        END as weekday_name,
    AVG(demand) AS avg_demand
    FROM CarSharing
    WHERE strftime('%Y', timestamp) = '2017'
    and weekday_name = (SELECT weekday_name from HighestLowestDemand WHERE avg_demand = (SELECT min(avg_demand) from HighestLowestDemand))
    GROUP BY hour, weekday_name
    ORDER BY avg_demand DESC"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_distinctweather_table(conn):
    # This function is part of the solution for question 7d
    # This function creates a distinct weather table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = '''CREATE TABLE distinctweather as SELECT distinct weather, weather_code from weather'''
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_temperature_frequency_table(conn):
    # This function is part of the solution for question 7d
    # This function creates temperature frequency table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = '''CREATE TABLE temperature_frequency AS SELECT temp_category, COUNT(*) AS count
                            FROM CarSharing a
                            WHERE strftime('%Y', a.timestamp) = '2017'
                            GROUP BY temp_category
                            ORDER BY count DESC'''
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_weather_frequency_table(conn):
    # This function is part of the solution for question 7d
    # This function creates weather frequency table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = '''CREATE TABLE weather_frequency AS SELECT b.weather, COUNT(*) AS count
                            FROM CarSharing a
                            LEFT JOIN distinctweather b
                            ON a.weather_code = b.weather_code
                            WHERE strftime('%Y', a.timestamp) = '2017'
                            GROUP BY b.weather
                            ORDER BY count DESC'''
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_windspeed_data_table(conn):
    # This function is part of the solution for question 7d
    # This function creates windspeed data table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE windspeed_data AS SELECT strftime('%m', timestamp) AS month, AVG(windspeed) AS average_windspeed, 
                        MAX(windspeed) AS highest_windspeed, MIN(windspeed) AS lowest_windspeed
                        FROM CarSharing
                        WHERE strftime('%Y', timestamp) = '2017'
                        AND windspeed != ''
                        GROUP BY month"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_humidity_data_table(conn):
    # This function is part of the solution for question 7d
    # This function creates humidity data table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE humidity_data AS SELECT strftime('%m', timestamp) AS month, AVG(humidity)
                        AS average_humidity, 
                        MAX(humidity) AS highest_humidity, MIN(humidity) AS lowest_humidity
                        FROM CarSharing
                        WHERE strftime('%Y', timestamp) = '2017'
                        AND humidity != ''
                        GROUP BY month"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_average_demand_by_temperature_table(conn):
    # This function is part of the solution for question 7d
    # This function creates an average demand by temperature table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE avg_demand_by_temp AS SELECT temp_category, AVG(demand) AS average_demand
                        FROM CarSharing
                        WHERE strftime('%Y', timestamp) = '2017'
                        And temp_category != ''
                        GROUP BY temp_category
                        ORDER BY average_demand DESC"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_monthly_summary_table(conn):
    # This function is part of the solution for question 7e
    # This function creates a monthly summary table.
    # The SQL Statement for this operation is in """ """
    create_table_sql = """CREATE TABLE monthly_summary AS SELECT a.temp_category, b.weather, AVG(a.humidity)
                        AS average_humidity, 
                        MAX(a.humidity) AS highest_humidity, MIN(a.humidity) AS lowest_humidity, 
                        AVG(windspeed) AS average_windspeed, 
                        MAX(a.windspeed) AS highest_windspeed, MIN(a.windspeed) AS lowest_windspeed, AVG(a.demand) AS average_demand, 
                        CASE strftime('%m', a.timestamp)
                                WHEN '01' THEN 'January'
                                WHEN '02' THEN 'February'
                                WHEN '03' THEN 'March'
                                WHEN '04' THEN 'April'
                                WHEN '05' THEN 'May'
                                WHEN '06' THEN 'June'
                                WHEN '07' THEN 'July'
                                WHEN '08' THEN 'August'
                                WHEN '09' THEN 'September'
                                WHEN '10' THEN 'October'
                                WHEN '11' THEN 'November'
                                WHEN '12' THEN 'December'
                        END AS month
                        FROM CarSharing a
                        LEFT JOIN distinctweather b
                        ON a.weather_code = b.weather_code
                        WHERE strftime('%Y', a.timestamp) = '2017'
                        GROUP BY month
                        ORDER BY average_demand DESC"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_highest_month_summary_table(conn):
    # This function is part of the solution for question 7e
    # This function creates an average demand by temperature table.
    # The SQL Statement for this operation is in """ """
    create_monthly_demand_average_table_sql = """CREATE TABLE monthly_demand_average AS SELECT
        CASE strftime('%m', timestamp)
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
            END AS month,
            AVG(demand) AS average_demand
        FROM CarSharing
        WHERE strftime('%Y', timestamp) = '2017'
        GROUP BY month
        ORDER BY average_demand DESC"""
    try:
        c = conn.cursor()
        c.execute(create_monthly_demand_average_table_sql)
    except Error as e:
        print(e)

    highest_month = """SELECT
        month from monthly_demand_average
    where month = (SELECT month from monthly_demand_average 
    WHERE average_demand = (SELECT max(average_demand) FROM monthly_demand_average))"""

    try:
        c = conn.cursor()
        month = c.execute(highest_month).fetchone()
        c.execute("CREATE TABLE highest_monthly_summary AS SELECT * FROM monthly_summary WHERE month = ?", month)
    except Error as e:
        print(e)


def main():
    database = r"D:\Programming\OmoT\cars.db"

    conn = create_connection(database)

    with conn:
        create_CarSharing_table(conn)
        import_csv_to_table(conn)
        print("CarSharing table created")
        create_CarSharingBackup_table(conn)
        print("Backup table created")

        print("Question 2\n")
        add_column(conn)
        print("temp_category column added")
        print("Question 3\n")
        create_temp_table(conn)
        print("temperature table created")
        print("Question 4\n")
        add_weathercode(conn)
        print("weather code and column added")
        print("Question 5\n")
        create_weather_table(conn)
        print("weather table created")
        print("Question 6\n")
        create_time_table(conn)
        print("time table created")
        print("Question 7a\n")
        cur = conn.cursor()
        que_7a = cur.execute(
            "SELECT timestamp FROM CarSharing WHERE demand=(SELECT MAX(demand) FROM CarSharing WHERE strftime('%Y', datetime(timestamp)) = '2017') GROUP BY timestamp HAVING (SELECT strftime('%Y', datetime(timestamp)) = '2017');").fetchone()
        print("Date and time with the highest demand rate in 2017: ", que_7a)
        print("Question 7b")
        create_car_average_demand_table(conn)
        create_highestlowestdemand_table(conn)
        print("Table with weekdays with highest and lowest demand created")
        query1 = "select * from HighestLowestDemand limit 20;"
        select_all(conn, query1)
        print("Question 7c")
        create_hourly_highest_average_demand_table(conn)
        print("Table with Hourly average demand for highest weekday created")
        query2 = "select * from HourlyHighestAverageDemand limit 20;"
        select_all(conn, query2)
        create_hourly_lowest_average_demand_table(conn)
        print("Table with Hourly average demand for lowest weekday created")
        query3 = "select * from HourlyLowestAverageDemand limit 20;"
        select_all(conn, query3)
        print("Question 7d")
        create_distinctweather_table(conn)
        print("Was it Mild, Hot or Cold")
        create_temperature_frequency_table(conn)
        print("Table with Temperature frequency created")
        query4 = "select * from temperature_frequency limit 20;"
        select_all(conn, query4)
        print("Which Weather condition was most prevalent")
        create_weather_frequency_table(conn)
        print("Table with the weather frequency created")
        query5 = "select * from weather_frequency limit 20;"
        select_all(conn, query5)
        print("Windspeed data summary")
        create_windspeed_data_table(conn)
        print("Table with windspeed data summary created")
        query6 = "select * from windspeed_data limit 20;"
        select_all(conn, query6)
        print("Humidity data summary")
        create_humidity_data_table(conn)
        print("Table with Humidity data summary created")
        query7 = "select * from humidity_data limit 20;"
        select_all(conn, query7)
        create_average_demand_by_temperature_table(conn)
        print("Table with average demand by temperature created")
        query8 = "select * from avg_demand_by_temp limit 20;"
        select_all(conn, query8)
        print("Question 7e")
        create_monthly_summary_table(conn)
        print("2017 monthly summary table created for comparism")
        query10 = "select * from monthly_summary limit 20;"
        select_all(conn, query10)
        create_highest_month_summary_table(conn)
        print("Monthly summary for month with the highest average demand created")
        query11 = "select * from highest_monthly_summary limit 20;"
        select_all(conn, query11)


if __name__ == '__main__':
    main()