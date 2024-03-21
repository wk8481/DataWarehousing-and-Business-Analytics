

import pyodbc
import pandas as pd
import requests
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN

# Define your Open-Meteo API parameters
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
API_PARAMS = {
    "temperature_unit": "celsius",
    "wind_speed_unit": "kmh",
    "precipitation_unit": "mm",
    "timeformat": "iso8601"
}

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
            rain_presence = row['rain_presence']

            insert_query = """
            INSERT INTO weather_history (city_id, date, rain_presence)
            VALUES (?, ?, ?)
            """
            values = (city_id, row['date'], rain_presence)
            print("Insert Query:", insert_query)
            print("Values:", values)
            cursor.execute(insert_query, values)

        cursor.commit()
        print("Weather data inserted into 'weather_history' table successfully.")

    except pyodbc.Error as e:
        print(f"Error inserting into 'weather_history' table: {e}")
        print(f"City ID: {city_id}")
        print(f"Weather data: {weather_data}")



def setup_weather_api(cursor, city):
    """
    Fetches weather data for a city from the Open-Meteo API and returns a DataFrame.

    Args:
        cursor (pyodbc.Cursor): Cursor object for making database queries.
        city (dict): Dictionary containing city information (city_id, latitude, longitude).

    Returns:
        pd.DataFrame: DataFrame containing processed hourly weather data for the city.
    """
    lat = city['latitude']
    lon = city['longitude']

    # Add the city's latitude and longitude to the API parameters
    params = {
        **API_PARAMS,
        "latitude": lat,
        "longitude": lon,
        "start_date": "2020-01-01",
        "end_date": "2024-03-20",  # Updated end_date
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain"]
    }

    try:
        response = requests.get(API_BASE_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            hourly_data = data.get("hourly")

            if not hourly_data:
                print(f"No hourly data found for city {city['city_id']}")
                return pd.DataFrame()

            # Print latitude and longitude
            print("Latitude:", lat)
            print("Longitude:", lon)

            # Print API response
            print("API Response for city ID:", city['city_id'])
            print(hourly_data)

            # Create DataFrame
            df = pd.DataFrame(hourly_data)

            # Convert 'time' to datetime
            df['time'] = pd.to_datetime(df['time'])

            # Extract needed columns
            df = df[['time', 'temperature_2m', 'relative_humidity_2m', 'rain']]

            # Convert temperature to Celsius
            df['temperature_2m'] = df['temperature_2m'].round(1) - 273.15

            # Convert rain to boolean (1 for rain, 0 for no rain)
            df['rain'] = (df['rain'] > 0).astype(int)

            # Rename columns
            df.columns = ['date', 'temperature_celsius', 'relative_humidity', 'rain_presence']

            return df
        else:
            print(f"Failed to retrieve data for city {city['city_id']}. Status Code: {response.status_code}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for city {city['city_id']}: {e}")
        return pd.DataFrame()
    except Exception as ex:
        print(f"An error occurred: {ex}")
        return pd.DataFrame()
def get_city_data(cursor):
    """
    Retrieves city data (latitude and longitude) from the operational database.

    Args:
        cursor (pyodbc.Cursor): Cursor object for the operational database connection.

    Returns:
        list: List of dictionaries containing city information (city_id, lat, lon).
    """
    try:
        cursor.execute("SELECT TOP 10 [city_id], [latitude], [longitude] FROM [catchem].[dbo].[city]")

        city_data = cursor.fetchall()

        cities = []
        for row in city_data:
            city_id_bytes = row[0]  # Get city_id as bytes
            city_id_str = city_id_bytes.hex()  # Convert bytes to hexadecimal string
            lat = row[1]
            lon = row[2]
            cities.append({'city_id': city_id_str, 'lat': lat, 'lon': lon})

        return cities

    except pyodbc.Error as e:
        print(f"Error fetching city data: {e}")
        return []

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


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
        # Here you would have your database connection setup
        conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
        cursor_op = conn_op.cursor()

        conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
        cursor_dwh = conn_dwh.cursor()

        # Create dimRain table
        create_dim_rain(cursor_dwh)

        # Sample city data
        cities = [
            {"city_id": 1, "city_name": "Brussels", "latitude": 50.8503, "longitude": 4.3517},
            {"city_id": 2, "city_name": "Antwerp", "latitude": 51.2195, "longitude": 4.4024},
            # Add more cities as needed
        ]

        for city in cities:
            print(f"Processing data for city ID: {city['city_id']}")
            weather_data = setup_weather_api(cursor_op, city)
            if not weather_data.empty:
                print(f"Weather data retrieved for city ID: {city['city_id']}")
                store_weather_history(cursor_dwh, weather_data, city['city_id'])
                fill_dim_rain(cursor_dwh, weather_data)
                print(f"Processing completed for city ID: {city['city_id']}")
            else:
                print(f"No weather data found for city ID: {city['city_id']}")

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