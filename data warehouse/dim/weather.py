import pyodbc
import pandas as pd
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
import requests


def setup_weather_api(log_df=None):  # Optional log_df parameter
    """
    Fetches weather data for each city based on log data (if provided) or directly from user input.

    Args:
        log_df (pd.DataFrame, optional): DataFrame containing 'lat' and 'lon' columns for weather retrieval (default: None).

    Returns:
        pd.DataFrame: DataFrame containing processed hourly weather data for all cities.
    """

    # Configure API session
    session = requests.Session()  # Use a non-caching session for this example

    # Required weather variables
    variables = ["relative_humidity_2m", "rain"]

    # Use log data for city locations if provided, otherwise prompt for user input
    if log_df is not None:
        cities = log_df[['lat', 'lon']]  # Assuming 'lat' and 'lon' columns exist
    else:
        num_cities = int(input("Enter the number of cities to retrieve weather for: "))
        cities = []
        for i in range(num_cities):
            lat = float(input(f"Enter latitude for city {i+1}: "))
            lon = float(input(f"Enter longitude for city {i+1}: "))
            cities.append({'lat': lat, 'lon': lon})

    all_hourly_data = pd.DataFrame()

    # Loop through each city and retrieve weather data
    for city in cities:
        lat = city['lat']
        lon = city['lon']

        # Get minimum and maximum dates for weather retrieval (adjust as needed)
        start_date = pd.to_datetime('today').strftime('%Y-%m-%d')  # Assuming today's date
        end_date = (pd.to_datetime('today') + pd.Timedelta(days=14)).strftime('%Y-%m-%d')  # Example: 14 days

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": variables
        }

        # Call API and handle responses
        response = session.get("https://archive-api.open-meteo.com/v1/archive", params=params)

        # Handle response status codes
        if response.status_code == 200:
            # Process successful response
            api_response = response.json()
            hourly_data = api_response.get("hourly")  # Assuming "hourly" key exists in the response

            if not hourly_data:
                print(f"Missing hourly data for city at ({lat}, {lon})")
                continue

            # Extract and reshape data for each variable
            for i, variable in enumerate(variables):
                try:
                    data = hourly_data.get(variable)  # Assuming variable data exists
                    if not data:
                        print(f"Missing data for variable '{variable}' in city at ({lat}, {lon})")
                        continue

                    # Assuming timestamps are in Unix seconds (adjust based on API documentation)
                    timestamps = data.get("time")  # Assuming "time" key exists
                    if not timestamps:
                        print(f"Missing timestamps for variable '{variable}' in city at ({lat}, {lon})")
                        continue

                    # Convert timestamps to datetime (adjust time zone handling if needed)
                    dates = pd.to_datetime(timestamps, unit="s")

                    # Create DataFrame with dates and variable data
                    df = pd.DataFrame({
                        "date": dates,
                        variable.replace("_", ""): data.get("values")  # Assuming "values" key exists
                    })

                    # Calculate rain presence based on a threshold (adjust as needed)
                    if variable == "rain":
                        rain_presence = pd.Series(df[variable] > 0, name="rain_presence")
                        df["rain_presence"] = rain_presence

                    # Append city's hourly data to all_hourly_data
                    all_hourly_data = all_hourly_data.append(df, ignore_index=True)
                except (KeyError, ValueError) as e:
                    print(f"Error processing data for variable '{variable}') in city at ({lat}, {lon}): {e}")

        else:
            print(f"Error retrieving data for city at ({lat}, {lon}): {response.status_code}")

    return all_hourly_data


def create_dim_weather(cursor_dwh):
    """Creates the 'dimWeather' table structure with rain category."""

    try:
        create_table_query = f"""
        CREATE TABLE dimRain (
   rain_id INT IDENTITY(1,1) PRIMARY KEY,
   rain_category INT NOT NULL,  -- 0: No Rain, 1: With Rain, 2: Unknown
   CONSTRAINT chk_rain_category CHECK (rain_category IN (0, 1, 2))  -- Enforce valid values
   )
        """
        cursor_dwh.execute(create_table_query)
        cursor_dwh.commit()
        print(f"Table 'dimWeather' created successfully.")

    except pyodbc.Error as e:
        print(f"Error creating 'dimWeather' table: {e}")


def fill_dim_weather(cursor_dwh, weather_data):
    """Fills the 'dimWeather' table with weather data and assigns rain categories."""

    try:
        # Logic for populating dimRain
        rain_data = []
        for index, row in weather_data.iterrows():
            rain_presence = row['rain_presence']

            # Assign rain category based on presence (adjust thresholds as needed)
            if rain_presence > 0.5:
                rain_category = 1  # With rain
            elif 0.5 >= rain_presence > 0:
                rain_category = 2  # Unknown
            else:
                rain_category = 0  # No rain

            rain_data.append({'rain_category': rain_category})

        # Insert data into dimRain
        for row in rain_data:
            insert_query = f"""
            INSERT INTO dimRain (rain_category)
            VALUES (?)
            """
            cursor_dwh.execute(insert_query, row['rain_category'])

        cursor_dwh.commit()
        print(f"Weather data inserted into 'dimWeather' table successfully.")
        print(f"Rain category data inserted into 'dimRain' table successfully.")

    except pyodbc.Error as e:
        print(f"Error inserting into tables: {e}")


def get_city_data(cursor_op):
    """
    Retrieves city data (latitude and longitude) from the operational database.

    Args:
        cursor_op (pyodbc.Cursor): Cursor object for the operational database connection.

    Returns:
        list: List of dictionaries containing city information (lat, lon).
    """

    # Execute the query to retrieve city data
    cursor_op.execute("SELECT TOP (1000) [city_id], [city_name], [latitude], [longitude] FROM [catchem].[dbo].[city]")

    # Fetch all results as a list of tuples
    city_data = cursor_op.fetchall()

    # Extract city information
    cities = []
    for row in city_data:
        city_id, city_name, lat, lon = row  # Assuming column order matches your query
        cities.append({'city_id': city_id, 'city_name': city_name, 'lat': lat, 'lon': lon})

    return cities

def main():
    try:
        # Connect to the 'catchem' database
        conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
        cursor_op = conn_op.cursor()

        # Connect to the 'catchem_dwh' database
        conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
        cursor_dwh = conn_dwh.cursor()

        # Retrieve city data from the operational database
        cities = get_city_data(cursor_op)

        # Retrieve weather data using retrieved city data
        weather_data = setup_weather_api(cities)  # Pass list of cities

        # Create the dimWeather table and fill with weather data (unchanged)
        create_dim_weather(cursor_dwh)
        fill_dim_weather(cursor_dwh, weather_data)

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()