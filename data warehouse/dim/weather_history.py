import requests
import pyodbc
from datetime import datetime, timedelta, date
import calendar
from config import SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER
from dwh import establish_connection

API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
API_PARAMS = {
    "temperature_unit": "celsius",
    "wind_speed_unit": "kmh",
    "precipitation_unit": "mm",
    "timeformat": "iso8601"
}

def create_weather_history_table(cursor):
    """Create the 'weather_history' table in the operational database."""
    try:
        create_history_query = """
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'weather_history'
        )
        BEGIN
        CREATE TABLE weather_history (
            id INT IDENTITY(1,1) PRIMARY KEY,
            date DATETIME,
            day INT,
            hour INT,
            city VARCHAR(255),
            weather_code INT,
            weather_type VARCHAR(50)
        )
        END
        """
        cursor.execute(create_history_query)
        print("weather_history table created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating weather_history table: {e}")

def categorize_weather_type(precipitation):
    """Categorize weather type based on precipitation."""
    if precipitation is None or precipitation < 1.0:
        return 'NO RAIN'
    else:
        return 'RAIN'

def retrieve_and_insert_hourly_weather_data(city_name, latitude, longitude, start_year, end_date, cursor):
    """Retrieve and insert hourly weather data for a city into 'weather_history'."""
    end_year = end_date.year
    yesterday = end_date - timedelta(days=1)

    for year in range(start_year, end_year + 1):
        end_month_iter = 13 if year < yesterday.year else yesterday.month + 1
        for month in range(1, end_month_iter):
            start_date = datetime(year, month, 1)
            end_date = datetime(year, month, calendar.monthrange(year, month)[1])

            if year == yesterday.year and month == yesterday.month:
                end_date = yesterday

            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "hourly": ["temperature_2m", "precipitation", "weathercode"]
            }

            response = requests.get(API_BASE_URL, params=params)
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
                for hour in range(24):
                    date_time = datetime(year, month, 1, hour, 0)
                    insert_query = """INSERT INTO weather_history (date,day,hour, city,weather_code, weather_type) 
                                                     VALUES (?, ?, ?,?,?,?)"""
                    cursor.execute(insert_query, (
                        date_time, date_time.day, date_time.hour,
                        city_name, None, 'UNKNOWN'))

    cursor.commit()

def main():
    # Establish connections
    conn_op = establish_connection()
    if conn_op:
        try:
            cursor_op = conn_op.cursor()

            # Create 'weather_history' table in the operational database
            create_weather_history_table(cursor_op)

            # Retrieve top 10 cities with the most treasures found
            popular_cities_query = """
                SELECT TOP (10) c.city_name, c.latitude, c.longitude
                FROM city c
                JOIN treasure t ON t.city_city_id = c.city_id
                JOIN treasure_log tl ON t.id = tl.treasure_id
                GROUP BY c.city_name, c.latitude, c.longitude
                ORDER BY COUNT(tl.log_time) DESC
                """
            cursor_op.execute(popular_cities_query)
            pop_cities = cursor_op.fetchall()

            for city_info in pop_cities:
                city_name, latitude, longitude = city_info
                retrieve_and_insert_hourly_weather_data(city_name, latitude, longitude, 2020, date.today(), cursor_op)

            print("Data insertion completed.")
        except pyodbc.Error as e:
            print(f"Error processing data: {e}")
        finally:
            conn_op.close()
    else:
        print("Connection to operational database failed.")

if __name__ == "__main__":
    main()
