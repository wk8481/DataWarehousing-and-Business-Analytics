#
#
#
#
#
#
# # import pyodbc
#
#
#
# # import pandas as pd
# # from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
# # import requests
# # import datetime
# #
# # def store_weather_history(cursor, weather_data, city_id):
# #     """
# #     Stores weather data into the 'weather_history' table.
# #
# #     Args:
# #         cursor (pyodbc.Cursor): Cursor object for the data warehouse database connection.
# #         weather_data (pd.DataFrame): DataFrame containing weather data.
# #         city_id (int): City ID for which the data is being stored.
# #     """
# #     try:
# #         for index, row in weather_data.iterrows():
# #             humidity = row['relative_humidity_2m']
# #             rain_presence = row['rain_presence']
# #
# #             insert_query = """
# #             INSERT INTO weather_history (city_id, date, humidity, rain_presence)
# #             VALUES (?, ?, ?, ?)
# #             """
# #             cursor.execute(insert_query, city_id, row['date'], humidity, rain_presence)
# #
# #         cursor.commit()
# #         print("Weather data inserted into 'weather_history' table successfully.")
# #
# #     except pyodbc.Error as e:
# #         print(f"Error inserting into 'weather_history' table: {e}")
# #
# # def get_city_data(cursor):
# #     """
# #     Retrieves city data (latitude and longitude) from the operational database.
# #
# #     Args:
# #         cursor (pyodbc.Cursor): Cursor object for the operational database connection.
# #
# #     Returns:
# #         list: List of dictionaries containing city information (city_id, lat, lon).
# #     """
# #     cursor.execute("SELECT TOP 10 [city_id], [latitude], [longitude] FROM [catchem].[dbo].[city]")
# #
# #     city_data = cursor.fetchall()
# #
# #     cities = [{'city_id': row[0], 'lat': row[1], 'lon': row[2]} for row in city_data]
# #
# #     return cities
# #
# # def setup_weather_api(session, city):
# #     """
# #     Fetches weather data for a city from the API and returns a DataFrame.
# #
# #     Args:
# #         session (requests.Session): Session object for making API requests.
# #         city (dict): Dictionary containing city information (city_id, lat, lon).
# #
# #     Returns:
# #         pd.DataFrame: DataFrame containing processed hourly weather data for the city.
# #     """
# #     lat = city['lat']
# #     lon = city['lon']
# #
# #     params = {
# #         "latitude": lat,
# #         "longitude": lon,
# #         "start_date": "2024-01-01",
# #         "end_date": "2024-01-02",
# #         "hourly": ["relative_humidity_2m", "rain"]
# #     }
# #
# #     response = session.get("https://archive-api.open-meteo.com/v1/archive", params=params)
# #
# #     if response.status_code == 200:
# #         data = response.json()
# #         hourly_data = data.get("hourly")
# #
# #         if not hourly_data:
# #             print(f"No hourly data found for city {city['city_id']}")
# #             return pd.DataFrame()
# #
# #         try:
# #             df = pd.DataFrame(hourly_data)
# #             df['rain_presence'] = (df['rain'] > 0).astype(int)
# #
# #             return df[['date', 'relative_humidity_2m', 'rain_presence']]  # Exclude 'date' column
# #         except KeyError as e:
# #             print(f"Error processing data for city {city['city_id']}: {e}")
# #             return pd.DataFrame()
# #
# #     else:
# #         print(f"Failed to retrieve data for city {city['city_id']}")
# #         return pd.DataFrame()
# #
# #
# # def create_dim_rain(cursor):
# #     """Creates the 'dimRain' table structure with rain category."""
# #
# #     try:
# #         cursor.execute("""
# #         IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dimRain')
# #         BEGIN
# #             CREATE TABLE dimRain (
# #                 rain_id INT IDENTITY(1,1) PRIMARY KEY,
# #                 rain_category INT NOT NULL,  -- 0: No Rain, 1: With Rain, 2: Unknown
# #                 CONSTRAINT chk_rain_category CHECK (rain_category IN (0, 1, 2))
# #             )
# #         END
# #         """)
# #         print("Table 'dimRain' created successfully.")
# #     except pyodbc.Error as e:
# #         print(f"Error creating 'dimRain' table: {e}")
# #
# #
# # def fill_dim_rain(cursor, weather_data):
# #     """Fills the 'dimRain' table with rain categories."""
# #
# #     try:
# #         rain_data = []
# #         for index, row in weather_data.iterrows():
# #             rain_presence = row['rain_presence']
# #
# #             if rain_presence > 0.5:
# #                 rain_category = 1  # With rain
# #             elif 0.5 >= rain_presence > 0:
# #                 rain_category = 2  # Unknown
# #             else:
# #                 rain_category = 0  # No rain
# #
# #             rain_data.append({'rain_category': rain_category})
# #
# #         for row in rain_data:
# #             insert_query = """
# #             INSERT INTO dimRain (rain_category)
# #             VALUES (?)
# #             """
# #             cursor.execute(insert_query, row['rain_category'])
# #
# #         cursor.commit()
# #         print("Rain category data inserted into 'dimRain' table successfully.")
# #
# #     except pyodbc.Error as e:
# #         print(f"Error inserting into 'dimRain' table: {e}")
# #
# #
# # def main():
# #     try:
# #         conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
# #         cursor_op = conn_op.cursor()
# #
# #         conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
# #         cursor_dwh = conn_dwh.cursor()
# #
# #         cities = get_city_data(cursor_op)
# #         session = requests.Session()
# #
# #         all_hourly_data = pd.DataFrame()
# #
# #         for city in cities:
# #             weather_data = setup_weather_api(session, city)
# #             if not weather_data.empty:
# #                 store_weather_history(cursor_dwh, weather_data, city['city_id'])
# #                 all_hourly_data = all_hourly_data.append(weather_data, ignore_index=True)
# #
# #         session.close()
# #
# #         create_dim_rain(cursor_dwh)
# #         fill_dim_rain(cursor_dwh, all_hourly_data)
# #
# #         conn_op.commit()
# #         conn_dwh.commit()
# #
# #         cursor_op.close()
# #         cursor_dwh.close()
# #         conn_op.close()
# #         conn_dwh.close()
# #
# #     except pyodbc.Error as e:
# #         print(f"Database error: {e}")
# #
# #     except Exception as e:
# #         print(f"An error occurred: {e}")
# #
# #
# # if __name__ == "__main__":
# #     main()
#
#
#
# import pyodbc
# import pandas as pd
# from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
# import requests
# import datetime
#
# def store_weather_history(cursor, weather_data, city_id):
#     """
#     Stores weather data into the 'weather_history' table.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for the data warehouse database connection.
#         weather_data (pd.DataFrame): DataFrame containing weather data.
#         city_id (int): City ID for which the data is being stored.
#     """
#     try:
#         for index, row in weather_data.iterrows():
#             humidity = row['relative_humidity_2m']
#             rain_presence = row['rain_presence']
#
#             insert_query = """
#             INSERT INTO weather_history (city_id, date, humidity, rain_presence)
#             VALUES (?, ?, ?, ?)
#             """
#             cursor.execute(insert_query, city_id, row['date'], humidity, rain_presence)
#
#         cursor.commit()
#         print("Weather data inserted into 'weather_history' table successfully.")
#
#     except pyodbc.Error as e:
#         print(f"Error inserting into 'weather_history' table: {e}")
#
# def get_city_data(cursor):
#     """
#     Retrieves city data (latitude and longitude) from the operational database.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for the operational database connection.
#
#     Returns:
#         list: List of dictionaries containing city information (city_id, lat, lon).
#     """
#     cursor.execute("SELECT TOP 10 [city_id], [latitude], [longitude] FROM [catchem].[dbo].[city]")
#
#     city_data = cursor.fetchall()
#
#     cities = [{'city_id': row[0], 'lat': row[1], 'lon': row[2]} for row in city_data]
#
#     return cities
#
# def setup_weather_api(cursor, city):
#     """
#     Fetches weather data for a city from the API and returns a DataFrame.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for making database queries.
#         city (dict): Dictionary containing city information (city_id, lat, lon).
#
#     Returns:
#         pd.DataFrame: DataFrame containing processed hourly weather data for the city.
#     """
#     lat = city['lat']
#     lon = city['lon']
#
#     params = {
#         "latitude": lat,
#         "longitude": lon,
#         "start_date": "2024-01-01",
#         "end_date": "2024-01-02",
#         "hourly": ["relative_humidity_2m", "rain"]
#     }
#
#     response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
#
#     if response.status_code == 200:
#         data = response.json()
#         hourly_data = data.get("hourly")
#
#         if not hourly_data:
#             print(f"No hourly data found for city {city['city_id']}")
#             return pd.DataFrame()
#
#         try:
#             df = pd.DataFrame(hourly_data)
#             df['rain_presence'] = (df['rain'] > 0).astype(int)
#
#             # Get log_time from treasure_log for this city
#             city_id = city['city_id']
#             cursor.execute("SELECT [log_time] FROM [catchem].[dbo].[treasure_log] WHERE [city_id] = ?", city_id)
#             log_times = cursor.fetchall()
#             log_times = [log_time[0] for log_time in log_times]  # Convert to list of datetime objects
#
#             # Assign log_time as 'date' column
#             df['date'] = log_times
#
#             return df[['date', 'relative_humidity_2m', 'rain_presence']]
#         except KeyError as e:
#             print(f"Error processing data for city {city['city_id']}: {e}")
#             return pd.DataFrame()
#
#     else:
#         print(f"Failed to retrieve data for city {city['city_id']}")
#         return pd.DataFrame()
#
#
# def create_dim_rain(cursor):
#     """Creates the 'dimRain' table structure with rain category."""
#
#     try:
#         cursor.execute("""
#         IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dimRain')
#         BEGIN
#             CREATE TABLE dimRain (
#                 rain_id INT IDENTITY(1,1) PRIMARY KEY,
#                 rain_category INT NOT NULL,  -- 0: No Rain, 1: With Rain, 2: Unknown
#                 CONSTRAINT chk_rain_category CHECK (rain_category IN (0, 1, 2))
#             )
#         END
#         """)
#         print("Table 'dimRain' created successfully.")
#     except pyodbc.Error as e:
#         print(f"Error creating 'dimRain' table: {e}")
#
#
# def fill_dim_rain(cursor, weather_data):
#     """Fills the 'dimRain' table with rain categories."""
#
#     try:
#         rain_data = []
#         for index, row in weather_data.iterrows():
#             rain_presence = row['rain_presence']
#
#             if rain_presence > 0.5:
#                 rain_category = 1  # With rain
#             elif 0.5 >= rain_presence > 0:
#                 rain_category = 2  # Unknown
#             else:
#                 rain_category = 0  # No rain
#
#             rain_data.append({'rain_category': rain_category})
#
#         for row in rain_data:
#             insert_query = """
#             INSERT INTO dimRain (rain_category)
#             VALUES (?)
#             """
#             cursor.execute(insert_query, row['rain_category'])
#
#         cursor.commit()
#         print("Rain category data inserted into 'dimRain' table successfully.")
#
#     except pyodbc.Error as e:
#         print(f"Error inserting into 'dimRain' table: {e}")
#
#
# def main():
#     try:
#         conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
#         cursor_op = conn_op.cursor()
#
#         conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
#         cursor_dwh = conn_dwh.cursor()
#
#         cities = get_city_data(cursor_op)
#         session = requests.Session()
#
#         all_hourly_data = pd.DataFrame()
#
#         for city in cities:
#             weather_data = setup_weather_api(cursor_op, city)
#             if not weather_data.empty:
#                 store_weather_history(cursor_dwh, weather_data, city['city_id'])
#                 all_hourly_data = all_hourly_data.append(weather_data, ignore_index=True)
#
#         session.close()
#
#         create_dim_rain(cursor_dwh)
#         fill_dim_rain(cursor_dwh, all_hourly_data)
#
#         conn_op.commit()
#         conn_dwh.commit()
#
#         cursor_op.close()
#         cursor_dwh.close()
#         conn_op.close()
#         conn_dwh.close()
#
#     except pyodbc.Error as e:
#         print(f"Database error: {e}")
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#
#
# if __name__ == "__main__":
#     main()
#
# import pyodbc
# import pandas as pd
# from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
# import requests
# import datetime
#
# def store_weather_history(cursor, weather_data, city_id):
#     """
#     Stores weather data into the 'weather_history' table.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for the data warehouse database connection.
#         weather_data (pd.DataFrame): DataFrame containing weather data.
#         city_id (int): City ID for which the data is being stored.
#     """
#     try:
#         for index, row in weather_data.iterrows():
#             humidity = row['relative_humidity_2m']
#             rain_presence = row['rain_presence']
#
#             insert_query = """
#             INSERT INTO weather_history (city_id, date, humidity, rain_presence)
#             VALUES (?, ?, ?, ?)
#             """
#             cursor.execute(insert_query, city_id, row['date'], humidity, rain_presence)
#
#         cursor.commit()
#         print("Weather data inserted into 'weather_history' table successfully.")
#
#     except pyodbc.Error as e:
#         print(f"Error inserting into 'weather_history' table: {e}")
#
# def get_city_data(cursor):
#     """
#     Retrieves city data (latitude and longitude) from the operational database.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for the operational database connection.
#
#     Returns:
#         list: List of dictionaries containing city information (city_id, lat, lon).
#     """
#     cursor.execute("SELECT TOP 10 [city_id], [latitude], [longitude] FROM [catchem].[dbo].[city]")
#
#     city_data = cursor.fetchall()
#
#     cities = [{'city_id': row[0], 'lat': row[1], 'lon': row[2]} for row in city_data]
#
#     return cities
#
# def setup_weather_api(cursor, city):
#     """
#     Fetches weather data for a city from the API and returns a DataFrame.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for making database queries.
#         city (dict): Dictionary containing city information (city_id, lat, lon).
#
#     Returns:
#         pd.DataFrame: DataFrame containing processed hourly weather data for the city.
#     """
#     lat = city['lat']
#     lon = city['lon']
#
#     params = {
#         "latitude": lat,
#         "longitude": lon,
#         "start_date": "2024-01-01",
#         "end_date": "2024-01-02",
#         "hourly": ["relative_humidity_2m", "rain"]
#     }
#
#     response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
#
#     if response.status_code == 200:
#         data = response.json()
#         hourly_data = data.get("hourly")
#
#         if not hourly_data:
#             print(f"No hourly data found for city {city['city_id']}")
#             return pd.DataFrame()
#
#         try:
#             df = pd.DataFrame(hourly_data)
#             df['rain_presence'] = (df['rain'] > 0).astype(int)
#
#             # Get log_time from treasure_log for this city
#             city_id = city['city_id']
#             cursor.execute("SELECT DISTINCT [log_time] FROM [catchem].[dbo].[treasure_log] WHERE [city_id] = ?", city_id)
#             log_times = cursor.fetchall()
#             log_times = [log_time[0] for log_time in log_times]  # Convert to list of datetime objects
#
#             # Assign log_time as 'date' column
#             df['date'] = log_times
#
#             # Add city_id to the DataFrame
#             df['city_id'] = city_id
#
#             return df[['city_id', 'date', 'relative_humidity_2m', 'rain_presence']]
#         except KeyError as e:
#             print(f"Error processing data for city {city['city_id']}: {e}")
#             return pd.DataFrame()
#
#     else:
#         print(f"Failed to retrieve data for city {city['city_id']}")
#         return pd.DataFrame()
#
#
# def create_dim_rain(cursor):
#     """Creates the 'dimRain' table structure with rain category."""
#
#     try:
#         cursor.execute("""
#         IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dimRain')
#         BEGIN
#             CREATE TABLE dimRain (
#                 rain_id INT IDENTITY(1,1) PRIMARY KEY,
#                 rain_category INT NOT NULL,  -- 0: No Rain, 1: With Rain, 2: Unknown
#                 CONSTRAINT chk_rain_category CHECK (rain_category IN (0, 1, 2))
#             )
#         END
#         """)
#         print("Table 'dimRain' created successfully.")
#     except pyodbc.Error as e:
#         print(f"Error creating 'dimRain' table: {e}")
#
#
# def fill_dim_rain(cursor, weather_data):
#     """Fills the 'dimRain' table with rain categories."""
#
#     try:
#         rain_data = []
#         for index, row in weather_data.iterrows():
#             rain_presence = row['rain_presence']
#
#             if rain_presence > 0.5:
#                 rain_category = 1  # With rain
#             elif 0.5 >= rain_presence > 0:
#                 rain_category = 2  # Unknown
#             else:
#                 rain_category = 0  # No rain
#
#             rain_data.append({'rain_category': rain_category})
#
#         for row in rain_data:
#             insert_query = """
#             INSERT INTO dimRain (rain_category)
#             VALUES (?)
#             """
#             cursor.execute(insert_query, row['rain_category'])
#
#         cursor.commit()
#         print("Rain category data inserted into 'dimRain' table successfully.")
#
#     except pyodbc.Error as e:
#         print(f"Error inserting into 'dimRain' table: {e}")
#
#
# def main():
#     try:
#         conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
#         cursor_op = conn_op.cursor()
#
#         conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
#         cursor_dwh = conn_dwh.cursor()
#
#         cities = get_city_data(cursor_op)
#         session = requests.Session()
#
#         all_hourly_data = pd.DataFrame()
#
#         for city in cities:
#             weather_data = setup_weather_api(cursor_op, city)
#             if not weather_data.empty:
#                 store_weather_history(cursor_dwh, weather_data, city['city_id'])
#                 all_hourly_data = all_hourly_data.append(weather_data, ignore_index=True)
#
#         session.close()
#
#         # create_dim_rain(cursor_dwh)
#         fill_dim_rain(cursor_dwh, all_hourly_data)
#
#         conn_op.commit()
#         conn_dwh.commit()
#
#         cursor_op.close()
#         cursor_dwh.close()
#         conn_op.close()
#         conn_dwh.close()
#
#     except pyodbc.Error as e:
#         print(f"Database error: {e}")
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#
#
# if __name__ == "__main__":
#     main()


# import pyodbc
# import pandas as pd
# import requests
# from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
#
# def create_dim_rain(cursor):
#     """Creates the 'dimRain' table structure with rain category."""
#     try:
#         cursor.execute("""
#         IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dimRain')
#         BEGIN
#             CREATE TABLE dimRain (
#                 rain_id INT IDENTITY(1,1) PRIMARY KEY,
#                 rain_category NVARCHAR(50) NOT NULL
#             )
#             INSERT INTO dimRain (rain_category) VALUES
#             ('No Rain'), ('With Rain'), ('Unknown')
#         END
#         """)
#         print("Table 'dimRain' created successfully.")
#     except pyodbc.Error as e:
#         print(f"Error creating 'dimRain' table: {e}")
#
# def setup_weather_api(cursor, city):
#     """
#     Fetches weather data for a city from the API and returns a DataFrame.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for making database queries.
#         city (dict): Dictionary containing city information (city_id, lat, lon).
#
#     Returns:
#         pd.DataFrame: DataFrame containing processed hourly weather data for the city.
#     """
#     lat = city['lat']
#     lon = city['lon']
#     start_date = "2024-01-01"
#     end_date = "2024-01-02"
#
#     base_url = "https://archive-api.open-meteo.com/v1/archive"
#
#     params = {
#         "latitude": lat,
#         "longitude": lon,
#         "start_date": start_date,
#         "end_date": end_date,
#         "hourly": ["relative_humidity_2m", "rain"]
#     }
#
#     response = requests.get(base_url, params=params)
#
#     if response.status_code == 200:
#         data = response.json()
#         hourly_data = data.get("hourly")
#
#         if not hourly_data:
#             print(f"No hourly data found for city {city['city_id']}")
#             return pd.DataFrame()
#
#         try:
#             df = pd.DataFrame(hourly_data)
#             df['rain_presence'] = (df['rain'] > 0).astype(int)
#             df['date'] = pd.to_datetime(df['time'], unit='s', origin='unix')  # Convert Unix timestamp to datetime
#
#             return df[['date', 'rain_presence']]
#         except KeyError as e:
#             print(f"Error processing data for city {city['city_id']}: {e}")
#             return pd.DataFrame()
#
#     else:
#         print(f"Failed to retrieve data for city {city['city_id']}")
#         return pd.DataFrame()
#
# def store_weather_history(cursor, weather_data, city_id):
#     """
#     Stores weather data into the 'weather_history' table.
#
#     Args:
#         cursor (pyodbc.Cursor): Cursor object for the data warehouse database connection.
#         weather_data (pd.DataFrame): DataFrame containing weather data.
#         city_id (int): City ID for which the data is being stored.
#     """
#     try:
#         for index, row in weather_data.iterrows():
#             rain_presence = row['rain_presence']
#
#             if rain_presence == 1:
#                 rain_category = 'With Rain'
#             elif rain_presence == 0:
#                 rain_category = 'No Rain'
#             else:
#                 rain_category = 'Unknown'
#
#             insert_query = """
#             INSERT INTO weather_history (city_id, date, humidity, rain_presence, rain_category)
#             VALUES (?, ?, ?, ?, ?)
#             """
#             cursor.execute(insert_query, city_id, row['date'], row.get('relative_humidity_2m', None), rain_presence, rain_category)
#
#         cursor.commit()
#         print("Weather data inserted into 'weather_history' table successfully.")
#
#     except pyodbc.Error as e:
#         print(f"Error inserting into 'weather_history' table: {e}")
#
# def main():
#     try:
#         conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
#         cursor_op = conn_op.cursor()
#
#         conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
#         cursor_dwh = conn_dwh.cursor()
#
#         create_dim_rain(cursor_dwh)
#
#         cities = [
#             {'city_id': 1, 'lat': 51.2194, 'lon': 4.4025},  # Example city data, you can add more cities
#             {'city_id': 2, 'lat': 40.7128, 'lon': -74.0060},
#             # Add more city data here...
#         ]
#
#         for city in cities:
#             weather_data = setup_weather_api(cursor_op, city)
#             if not weather_data.empty:
#                 store_weather_history(cursor_dwh, weather_data, city['city_id'])
#
#         conn_dwh.commit()
#
#         cursor_op.close()
#         cursor_dwh.close()
#         conn_op.close()
#         conn_dwh.close()
#
#     except pyodbc.Error as e:
#         print(f"Database error: {e}")
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#
# if __name__ == "__main__":
#     main()


import pyodbc
import pandas as pd
import requests
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from datetime import datetime

def store_weather_history(cursor, weather_data, city_id):
    """
    Stores weather data into the 'weather_history' table.

    Args:
        cursor (pyodbc.Cursor): Cursor object for the data warehouse database connection.
        weather_data (pd.DataFrame): DataFrame containing weather data.
        city_id (int): City ID for which the data is being stored.
    """
    try:
        for index, row in weather_data.iterrows():
            humidity = row['relative_humidity_2m']
            rain_presence = row['rain_presence']

            insert_query = """
            INSERT INTO weather_history (city_id, date, humidity, rain_presence)
            VALUES (?, ?, ?, ?)
            """
            cursor.execute(insert_query, city_id, row['date'], humidity, rain_presence)

        cursor.commit()
        print("Weather data inserted into 'weather_history' table successfully.")

    except pyodbc.Error as e:
        print(f"Error inserting into 'weather_history' table: {e}")

def setup_weather_api(cursor, city):
    """
    Fetches weather data for a city from the API and returns a DataFrame.

    Args:
        cursor (pyodbc.Cursor): Cursor object for making database queries.
        city (dict): Dictionary containing city information (city_id, lat, lon).

    Returns:
        pd.DataFrame: DataFrame containing processed hourly weather data for the city.
    """
    lat = city['lat']
    lon = city['lon']

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "hourly": ["relative_humidity_2m", "rain"]
    }

    response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)

    if response.status_code == 200:
        data = response.json()
        hourly_data = data.get("hourly")

        if not hourly_data:
            print(f"No hourly data found for city {city['city_id']}")
            return pd.DataFrame()

        try:
            df = pd.DataFrame(hourly_data)
            df['rain_presence'] = (df['rain'] > 0).astype(int)
            df['date'] = df['time'].apply(lambda x: datetime.fromtimestamp(x / 1000))  # Convert ms to seconds
            df['date'] = df['date'].dt.round('H')  # Round to the nearest hour

            return df[['date', 'rain_presence']]
        except KeyError as e:
            print(f"Error processing data for city {city['city_id']}: {e}")
            return pd.DataFrame()

    else:
        print(f"Failed to retrieve data for city {city['city_id']}")
        return pd.DataFrame()
def get_city_data(cursor):
    """
    Retrieves city data (latitude and longitude) from the operational database.

    Args:
        cursor (pyodbc.Cursor): Cursor object for the operational database connection.

    Returns:
        list: List of dictionaries containing city information (city_id, lat, lon).
    """
    cursor.execute("SELECT TOP 10 [city_id], [latitude], [longitude] FROM [catchem].[dbo].[city]")

    city_data = cursor.fetchall()

    cities = [{'city_id': row[0], 'lat': row[1], 'lon': row[2]} for row in city_data]

    return cities

def create_dim_rain(cursor):
    """Creates the 'dimRain' table structure with rain category."""

    try:
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dimRain')
        BEGIN
            CREATE TABLE dimRain (
                rain_id INT IDENTITY(1,1) PRIMARY KEY,
                rain_category INT NOT NULL,  -- 0: No Rain, 1: With Rain, 2: Unknown
                CONSTRAINT chk_rain_category CHECK (rain_category IN (0, 1, 2))
            )
        END
        """)
        print("Table 'dimRain' created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating 'dimRain' table: {e}")

def fill_dim_rain(cursor, weather_data):
    """Fills the 'dimRain' table with rain categories."""

    try:
        rain_data = []
        for index, row in weather_data.iterrows():
            rain_presence = row['rain_presence']

            if rain_presence > 0.5:
                rain_category = 1  # With rain
            elif 0.5 >= rain_presence > 0:
                rain_category = 2  # Unknown
            else:
                rain_category = 0  # No rain

            rain_data.append({'rain_category': rain_category})

        for row in rain_data:
            insert_query = """
            INSERT INTO dimRain (rain_category)
            VALUES (?)
            """
            cursor.execute(insert_query, row['rain_category'])

        cursor.commit()
        print("Rain category data inserted into 'dimRain' table successfully.")

    except pyodbc.Error as e:
        print(f"Error inserting into 'dimRain' table: {e}")

def main():
    try:
        conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
        cursor_op = conn_op.cursor()

        conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
        cursor_dwh = conn_dwh.cursor()

        # Create dimRain table
        create_dim_rain(cursor_dwh)

        cities = get_city_data(cursor_op)

        for city in cities:
            weather_data = setup_weather_api(cursor_op, city)
            if not weather_data.empty:
                store_weather_history(cursor_dwh, weather_data, city['city_id'])
                fill_dim_rain(cursor_dwh, weather_data)

        conn_op.commit()
        conn_dwh.commit()

        cursor_op.close()
        cursor_dwh.close()
        conn_op.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        print(f"Database error: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
