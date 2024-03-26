import requests
import pyodbc
import pandas as pd
from datetime import datetime, timedelta, date
import calendar

# Update these connection details to match your database
driver = 'ODBC Driver 18 for SQL Server'
server = 'localhost,1533'
dsn = 'sqlserver'
user = 'SA'
password = 'DB3&3DB&DB3'
database = 'catchem'
DWH = 'catchem_dwh'
con_string = f'DRIVER={driver};SERVER={server};DSN={dsn};UID={user};PWD={password};DATABASE={database};TrustServerCertificate=yes'
con_DWH = f'DRIVER={driver};SERVER={server};DSN={dsn};UID={user};PWD={password};DATABASE={DWH};TrustServerCertificate=yes'
cursor = None
cursorDWH = None


def create_weather_table(cursor):
    try:
        create_query = """
        CREATE TABLE weather_history (
            id INT IDENTITY(1,1) PRIMARY KEY,
            date DATETIME,
            day INT,
            hour INT,
            city VARCHAR(255),
            weather_code INT,
            weather_type VARCHAR(50)
        )
        """
        cursor.execute(create_query)
        print("weather_history table created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating weather_history table: {e}")


# Function to categorize weather type based on precipitation
def categorize_weather_type(precipitation):
    if precipitation is None or precipitation < 1.0:
        return 'NO RAIN'
    else:
        return 'RAIN'


def fetch_and_insert_hourly_weather_data(city_name, latitude, longitude, start_year, end_date, cursor):
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    end_year = end_date.year

    yesterday = end_date - timedelta(days=1)
    for year in range(start_year, end_year + 1):
        end_month_iter = 13 if year < yesterday.year else yesterday.month + 1
        for month in range(1, end_month_iter):
            start_date = datetime(year, month, 1)
            end_date = datetime(year, month, calendar.monthrange(year, month)[1])  # Getting last day of the month

            # Adjusting the end date within the month to yesterday's date if applicable
            if year == yesterday.year and month == yesterday.month:
                end_date = yesterday

            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "hourly": ["temperature_2m", "precipitation", "weathercode"]
            }

            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                for time, temperature, precipitation, weather_code in zip(data['hourly']['time'],
                                                                          data['hourly']['temperature_2m'],
                                                                          data['hourly']['precipitation'],
                                                                          data['hourly']['weathercode']):
                    weather_type = categorize_weather_type(precipitation)
                    datetime_obj = datetime.strptime(time, '%Y-%m-%dT%H:%M')
                    insert_query = """INSERT INTO weather_history (date,day,hour, city,weather_code, weather_type) 
                                      VALUES (?, ?, ?,?,?,?)"""
                    cursor.execute(insert_query, (
                        datetime_obj, datetime_obj.day, datetime_obj.hour,
                        city_name, weather_code, weather_type))
            else:
                print(f"Failed to fetch data for {city_name} in {year}-{month}: {response.status_code}")
                # Insert 'UNKNOWN' for each hour of the day if data is not available
                for hour in range(24):
                    date_time = datetime(year, month, 1, hour, 0)
                    insert_query = """INSERT INTO weather_history (date,day,hour, city,weather_code, weather_type) 
                                                     VALUES (?, ?, ?,?,?,?)"""
                    cursor.execute(insert_query, (
                        date_time, date_time.day, date_time.hour,
                        city_name, None, 'UNKNOWN'))

    cursor.commit()


def main():
    try:
        cnxn = pyodbc.connect(con_string)
        cursor = cnxn.cursor()
        print(' Successfully connected to the DataBase-> ', con_string)

        connectDWH = pyodbc.connect(con_DWH)
        print(' Successfully connected to the DataWH-> ', DWH)
        cursorDWH = connectDWH.cursor()

        # Retrieve the top 10 cities with the most treasures found
        top_cities_query = """
                SELECT TOP (10) c.city_name, c.latitude, c.longitude
                FROM city c
                JOIN treasure t ON t.city_city_id = c.city_id
                JOIN treasure_log tl ON t.id = tl.treasure_id
                GROUP BY c.city_name, c.latitude, c.longitude
                ORDER BY COUNT(tl.log_time) DESC
                """
        cursor.execute(top_cities_query)
        top_cities = cursor.fetchall()

        create_weather_table(cursor)

        for city_info in top_cities:
            city_name, latitude, longitude = city_info
            fetch_and_insert_hourly_weather_data(city_name, latitude, longitude, 2020, date.today(), cursor)

        # Close the connections
        cnxn.close()
        cursor.close()
        connectDWH.close()
        cursorDWH.close()

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")


if __name__ == "__main__":
    main()
