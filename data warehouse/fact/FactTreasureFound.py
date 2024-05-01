import pyodbc
import datetime
from dwh import establish_connection
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER

def get_top_cities(cursor_op):
    """
    Fetches the top 10 cities from the weather_history table.
    """
    top_cities_query = """
    SELECT DISTINCT TOP 10 city
    FROM weather_history
    ORDER BY city
    """
    cursor_op.execute(top_cities_query)
    top_cities = [row[0] for row in cursor_op.fetchall()]
    return top_cities

def create_fact_table(cursor):
    # Create the FactTreasureFound table if it doesn't exist
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FactTreasureFound')
    BEGIN
        CREATE TABLE FactTreasureFound (
            TREASURE_ID INT PRIMARY KEY,
            DIM_DAY_SK INT,
            DIM_TREASURE_TYPE_SK INT,
            DIM_USER_SK INT,
            RAIN_ID INT,
            TOTAL_CACHES INT,
            CREATED_AT DATETIME,
            DEFAULT_MEASUREMENT INT DEFAULT 1,
            DURATION_OF_QUEST INT,
            CONSTRAINT fk_dim_day FOREIGN KEY (DIM_DAY_SK) REFERENCES dimDay(day_SK),
            CONSTRAINT fk_dim_treasure_type FOREIGN KEY (DIM_TREASURE_TYPE_SK) REFERENCES dimTreasureType(treasureType_SK),
            CONSTRAINT fk_dim_user FOREIGN KEY (DIM_USER_SK) REFERENCES dimUser(user_SK),
            CONSTRAINT fk_dim_rain FOREIGN KEY (RAIN_ID) REFERENCES dimRain(rain_id)
        );
    END
    """)
def lookup_dim_day_sk(cursor_dwh, log_time):
    """
    Look up DIM_DAY_SK based on log_time.
    """
    dimDate_query = "SELECT day_SK FROM dimDay WHERE [Date] = ?"
    cursor_dwh.execute(dimDate_query, log_time.strftime('%Y-%m-%d'))
    result = cursor_dwh.fetchone()
    if result:
        return result[0]  # Return DIM_DAY_SK if found
    else:
        return None  # Return None if not found

def lookup_dim_user_sk(cursor_dwh, hunter_id):
    """
    Look up DIM_USER_SK based on hunter_id.
    """
    dimUser_query = "SELECT user_SK FROM dimUser WHERE userId = ? AND scd_active = 1"
    cursor_dwh.execute(dimUser_query, hunter_id)
    result = cursor_dwh.fetchone()
    if result:
        return result[0]  # Return DIM_USER_SK if found
    else:
        return None  # Return None if not found

def lookup_dim_treasure_type_sk(cursor_dwh, difficulty, size, terrain, visibility):
    """
    Look up DIM_TREASURE_TYPE_SK based on the combination of difficulty, size, terrain, and visibility.
    """
    dimTreasure_query = """
    SELECT treasureType_SK
    FROM dimTreasureType
    WHERE difficulty = ? AND size = ? AND terrain = ? AND visibility = ?
    """
    cursor_dwh.execute(dimTreasure_query, (difficulty, size, terrain, visibility))
    result = cursor_dwh.fetchone()
    if result:
        return result[0]  # Return DIM_TREASURE_TYPE_SK if found
    else:
        return None  # Return None if not found

def lookup_rain_id(cursor_dwh, weather_type):
    """
    Look up RAIN_ID based on weather_type.
    """
    rain_id_query = "SELECT rain_id FROM dimRain WHERE rain_category = ?"
    cursor_dwh.execute(rain_id_query, weather_type)
    result = cursor_dwh.fetchone()
    if result:
        return result[0]  # Return RAIN_ID if found
    else:
        return None  # Return None if not found

def insert(cursor_op, cursor_dwh):
    # Fetch the top 10 cities
    top_cities = get_top_cities(cursor_op)

    for city_name in top_cities:
        # Fetch data from operational database treasure_log table for the current city
        treasure_log_query = """
        SELECT tl.id, tl.log_time, tl.hunter_id, tl.treasure_id, tl.session_start, tr.difficulty, st.size, tr.terrain, st.visibility
        FROM treasure_log tl
        INNER JOIN treasure tr ON tl.treasure_id = tr.treasure_id
        INNER JOIN stage st ON tl.stage_id = st.stage_id
        INNER JOIN weather_history wh ON tr.city_id = wh.city_id
        WHERE tl.log_type = 2 AND wh.city = ?
        """
        cursor_op.execute(treasure_log_query, (city_name,))

        for row in cursor_op.fetchall():
            log_id, log_time, hunter_id, treasure_id, session_start, difficulty, size, terrain, visibility = row

            # Convert string representations to datetime objects with milliseconds
            log_time = log_time.replace(microsecond=0)  # Remove milliseconds
            session_start = session_start.replace(microsecond=0) if session_start else None

            # Check if the record already exists in the fact table
            factTreasureFound_query = "SELECT TREASURE_ID FROM FactTreasureFound WHERE TREASURE_ID = ?"
            cursor_dwh.execute(factTreasureFound_query, treasure_id)

            if not cursor_dwh.fetchone():  # If the record doesn't exist, we want to only insert SK
                # Lookup DIM_DAY_SK
                dimDate_SK = lookup_dim_day_sk(cursor_dwh, log_time)
                if dimDate_SK is None:
                    print("DIM_DAY_SK not found. Skipping record.")
                    continue

                # Lookup DIM_USER_SK
                dimUser_SK = lookup_dim_user_sk(cursor_dwh, hunter_id)
                if dimUser_SK is None:
                    print("DIM_USER_SK not found. Skipping record.")
                    continue

                # Lookup treasure type SK based on difficulty, size, terrain, and visibility
                treasure_type_sk = lookup_dim_treasure_type_sk(cursor_dwh, difficulty, size, terrain, visibility)
                if treasure_type_sk is None:
                    print("DIM_TREASURE_TYPE_SK not found. Skipping record.")
                    continue

                # Lookup weather information from weather_history
                weather_query = """
                SELECT TOP 1 city, weather_code, weather_type
                FROM weather_history
                WHERE city = ? AND date <= ?
                ORDER BY date DESC
                """
                cursor_op.execute(weather_query, (city_name, log_time))
                weather_info = cursor_op.fetchone()

                if weather_info:
                    city, weather_code, weather_type = weather_info
                else:
                    city, weather_code, weather_type = None, None, "Rain situation unknown"

                # Lookup rain_id from dimRain based on weather_type
                rain_id = lookup_rain_id(cursor_dwh, weather_type)
                if rain_id is None:
                    print("RAIN_ID not found. Skipping record.")
                    continue

                # Lookup TOTAL_CACHES from treasure_log
                total_caches_query = "SELECT COUNT(*) FROM treasure_log WHERE log_type = 2"
                cursor_op.execute(total_caches_query)
                total_caches = cursor_op.fetchone()[0]

                # Calculate Duration of the Quest
                duration_of_quest = (session_start - log_time).total_seconds() if session_start else None

                # Insert into FactTreasureFound
                insert_query = """
                INSERT INTO FactTreasureFound (TREASURE_ID, DIM_DAY_SK, DIM_TREASURE_TYPE_SK, DIM_USER_SK, RAIN_ID, TOTAL_CACHES, CREATED_AT, DEFAULT_MEASUREMENT, DURATION_OF_QUEST)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)  -- Set DEFAULT_MEASUREMENT to 1
                """
                cursor_dwh.execute(insert_query,
                                   (treasure_id, dimDate_SK, treasure_type_sk, dimUser_SK, rain_id, total_caches, log_time, duration_of_quest))

    # Commit the transaction
    cursor_dwh.commit()

def job_fill_fact_table(cursor_op, cursor_dwh):
    try:
        # Empty the FactTreasureFound table
        cursor_dwh.execute("DELETE FROM FactTreasureFound")
        cursor_dwh.commit()

        # Fill the FactTreasureFound table for all cities
        insert(cursor_op, cursor_dwh)
        print("FactTreasureFound table filled.")

    except pyodbc.Error as e:
        print(f"Error in job_fill_fact_table: {e}")

def main():
    global cursor_op, cursor_dwh, conn_op, conn_dwh
    try:
        conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

        cursor_op = conn_op.cursor()
        cursor_dwh = conn_dwh.cursor()

        # Create the FactTreasureFound table if it doesn't exist
        create_fact_table(cursor_dwh)

        # Run the job to empty and fill the FactTreasureFound table
        job_fill_fact_table(cursor_op, cursor_dwh)

    except pyodbc.Error as e:
        print(f"Error in main function: {e}")

    finally:
        if cursor_op:
            cursor_op.close()
        if cursor_dwh:
            cursor_dwh.close()
        if conn_op:
            conn_op.close()
        if conn_dwh:
            conn_dwh.close()

        print("Data insertion completed.")

if __name__ == "__main__":
    main()
