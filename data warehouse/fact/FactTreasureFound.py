import pyodbc
from dwh import establish_connection
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER

def create_fact_table(cursor):
    # Create the FactTreasureFound table
    cursor.execute("""
    CREATE TABLE FactTreasureFound (
        TREASURE_ID INT PRIMARY KEY,
        DIM_DAY_SK INT,
        DIM_TREASURE_TYPE_SK INT,
        DIM_USER_SK INT,
        RAIN_ID INT,
        TOTAL_CACHES INT,
        CREATED_AT DATETIME
    )
    """)

def main():
    try:
        # Create connection strings
        conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

        # Create cursor objects
        cursor_op = conn_op.cursor()
        cursor_dwh = conn_dwh.cursor()

        # Create the FactTreasureFound table
        create_fact_table(cursor_dwh)

        # Fetch data from operational database treasure_log table
        treasure_log_query = """
        SELECT tl.id, tl.log_time, tl.hunter_id, tl.treasure_id, c.city_name
        FROM treasure_log tl
        JOIN city c ON tl.treasure_id = c.city_id
        WHERE tl.log_type = 2
        """
        cursor_op.execute(treasure_log_query)
        for row in cursor_op.fetchall():
            log_id, log_time, hunter_id, treasure_id, city_name = row

            # Check if the record already exists in the fact table
            factTreasureFound_query = "SELECT TREASURE_ID FROM FactTreasureFound WHERE TREASURE_ID = ?"
            cursor_dwh.execute(factTreasureFound_query, treasure_id)

            if not cursor_dwh.fetchone():  # If the record doesn't exist, we want to only insert SK
                # Fetch DIM_Day_SK
                dimDay_query = "SELECT day_SK FROM dimDay WHERE Date = ?"
                cursor_dwh.execute(dimDay_query, log_time)
                day_SK = cursor_dwh.fetchone()

                if day_SK:
                    day_SK = day_SK[0]
                else:
                    # Handle the case when day_SK is not found
                    print(f"Error: day_SK not found for date: {log_time}")
                    continue

                # Fetch DIM_TreasureType_SK
                dimTreasureType_query = "SELECT treasureType_SK FROM dimTreasureType WHERE treasure_id = ?"
                cursor_dwh.execute(dimTreasureType_query, treasure_id)
                dimTreasureType_SK = cursor_dwh.fetchone()

                if dimTreasureType_SK:
                    dimTreasureType_SK = dimTreasureType_SK[0]
                else:
                    # Handle the case when dimTreasureType_SK is not found
                    print(f"Error: dimTreasureType_SK not found for treasure_id: {treasure_id}")
                    continue

                # Fetch DIM_User_SK (considering SCD attributes)
                dimUser_query = """
                SELECT user_SK
                FROM dimUser
                WHERE userId = ?
                AND scd_active = 1
                """
                cursor_dwh.execute(dimUser_query, hunter_id)
                dimUser_SK = cursor_dwh.fetchone()

                if dimUser_SK:
                    dimUser_SK = dimUser_SK[0]
                else:
                    # If user not found with scd_active = 1, check if there's a historical record
                    historical_user_query = """
                    SELECT user_SK
                    FROM dimUser
                    WHERE userId = ?
                    AND scd_active = 0
                    AND scd_end >= ?
                    """
                    cursor_dwh.execute(historical_user_query, (hunter_id, log_time))
                    historical_user_SK = cursor_dwh.fetchone()

                    if historical_user_SK:
                        # Reactivate the historical user record
                        reactivate_user_query = """
                        UPDATE dimUser
                        SET scd_active = 1,
                            scd_end = NULL,
                            scd_version = scd_version + 1
                        WHERE user_SK = ?
                        """
                        cursor_dwh.execute(reactivate_user_query, historical_user_SK[0])
                        dimUser_SK = historical_user_SK[0]
                    else:
                        # Handle the case when dimUser_SK is not found
                        print(f"Error: dimUser_SK not found for hunter_id: {hunter_id}")
                        continue

                # Fetch weather information from weather_history
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
                    # If weather info is not found, link to "Rain situation unknown"
                    city, weather_code, weather_type = None, None, "Rain situation unknown"

                # Fetch rain_id from dimRain based on weather_type
                rain_id_query = """
                SELECT rain_id
                FROM dimRain
                WHERE rain_category = ?
                """
                cursor_dwh.execute(rain_id_query, weather_type)
                rain_id = cursor_dwh.fetchone()

                if rain_id:
                    rain_id = rain_id[0]
                else:
                    # Handle the case when rain_id is not found
                    print(f"Error: rain_id not found for weather_type: {weather_type}")
                    continue

                # Fetch TOTAL_CACHES from treasure_log
                total_caches_query = """
                SELECT COUNT(*)
                FROM treasure_log
                WHERE log_type = 2
                """
                cursor_op.execute(total_caches_query)
                total_caches = cursor_op.fetchone()[0]

                # Insert into FactTreasureFound
                insert_query = """
                INSERT INTO FactTreasureFound (TREASURE_ID, DIM_DAY_SK, DIM_TREASURE_TYPE_SK, DIM_USER_SK, RAIN_ID, TOTAL_CACHES, CREATED_AT)
                VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                """
                cursor_dwh.execute(insert_query,
                                   (treasure_id, day_SK, dimTreasureType_SK, dimUser_SK, rain_id, total_caches))

        conn_dwh.commit()
        cursor_dwh.close()
        # close connections
        conn_op.close()
        conn_dwh.close()

        print("Data insertion completed.")

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()
