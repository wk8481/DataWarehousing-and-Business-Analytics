import pyodbc
from dwh import establish_connection
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER

def create_fact_table(cursor):
    # Create the FactTreasureFound table if it doesn't exist
    cursor.execute("""
    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = 'FactTreasureFound'
    )
    BEGIN
        CREATE TABLE FactTreasureFound (
            TREASURE_ID INT,
            DIM_DAY_SK INT,
            DIM_TREASURE_TYPE_SK INT,
            DIM_USER_SK INT,
            RAIN_ID INT,
            TOTAL_CACHES INT,
            CREATED_AT DATETIME,
            FOREIGN KEY (TREASURE_ID) REFERENCES Treasure(id),
            FOREIGN KEY (DIM_DAY_SK) REFERENCES dimDay(day_SK),
            FOREIGN KEY (DIM_TREASURE_TYPE_SK) REFERENCES dimTreasureType(treasureType_SK),
            FOREIGN KEY (DIM_USER_SK) REFERENCES dimUser(user_SK),
            FOREIGN KEY (RAIN_ID) REFERENCES dimRain(rain_id)
        )
    END
    """)

def get_dim_day_sk(cursor, log_time):
    # Fetch DIM_Day_SK
    dimDay_query = "SELECT day_SK FROM dimDay WHERE Date = ?"
    cursor.execute(dimDay_query, log_time)
    day_SK = cursor.fetchone()

    if day_SK:
        return day_SK[0]
    else:
        # Handle the case when day_SK is not found
        print(f"Error: day_SK not found for date: {log_time}")
        return None

def get_dim_treasure_type_sk(cursor, treasure_id):
    # Fetch DIM_TreasureType_SK
    dimTreasureType_query = "SELECT treasureType_SK FROM dimTreasureType WHERE treasure_id = ?"
    cursor.execute(dimTreasureType_query, treasure_id)
    dimTreasureType_SK = cursor.fetchone()

    if dimTreasureType_SK:
        return dimTreasureType_SK[0]
    else:
        # Handle the case when dimTreasureType_SK is not found
        print(f"Error: dimTreasureType_SK not found for treasure_id: {treasure_id}")
        return None

def get_dim_user_sk(cursor, hunter_id, log_time):
    # Fetch DIM_User_SK (considering SCD attributes)
    dimUser_query = """
    SELECT user_SK
    FROM dimUser
    WHERE userId = ?
    AND scd_active = 1
    """
    cursor.execute(dimUser_query, hunter_id)
    dimUser_SK = cursor.fetchone()

    if dimUser_SK:
        return dimUser_SK[0]
    else:
        # If user not found with scd_active = 1, check if there's a historical record
        historical_user_query = """
        SELECT user_SK
        FROM dimUser
        WHERE userId = ?
        AND scd_active = 0
        AND scd_end >= ?
        """
        cursor.execute(historical_user_query, (hunter_id, log_time))
        historical_user_SK = cursor.fetchone()

        if historical_user_SK:
            # Reactivate the historical user record
            reactivate_user_query = """
            UPDATE dimUser
            SET scd_active = 1,
                scd_end = NULL,
                scd_version = scd_version + 1
            WHERE user_SK = ?
            """
            cursor.execute(reactivate_user_query, historical_user_SK[0])
            return historical_user_SK[0]
        else:
            # Handle the case when dimUser_SK is not found
            print(f"Error: dimUser_SK not found for hunter_id: {hunter_id}")
            return None

def get_rain_id(cursor, weather_type):
    # Fetch rain_id from dimRain based on weather_type
    rain_id_query = """
    SELECT rain_id
    FROM dimRain
    WHERE rain_category = ?
    """
    cursor.execute(rain_id_query, weather_type)
    rain_id = cursor.fetchone()

    if rain_id:
        return rain_id[0]
    else:
        # Handle the case when rain_id is not found
        print(f"Error: rain_id not found for weather_type: {weather_type}")
        return None

def get_total_caches(cursor):
    # Fetch TOTAL_CACHES from treasure_log
    total_caches_query = """
    SELECT COUNT(*)
    FROM treasure_log
    WHERE log_type = 2
    """
    cursor.execute(total_caches_query)
    total_caches = cursor.fetchone()

    if total_caches:
        return total_caches[0]
    else:
        # Handle the case when total_caches is not found
        print("Error: Total caches not found.")
        return None

def insert_fact_table(cursor, treasure_id, day_SK, dimTreasureType_SK, dimUser_SK, rain_id, total_caches, log_time):
    # Insert into FactTreasureFound
    insert_query = """
    INSERT INTO FactTreasureFound (TREASURE_ID, DIM_DAY_SK, DIM_TREASURE_TYPE_SK, DIM_USER_SK, RAIN_ID, TOTAL_CACHES, CREATED_AT)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (treasure_id, day_SK, dimTreasureType_SK, dimUser_SK, rain_id, total_caches, log_time))

def main():
    try:
        # Create connection strings
        conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

        # Create cursor objects
        cursor_op = conn_op.cursor()
        cursor_dwh = conn_dwh.cursor()

        # Create the FactTreasureFound table if it doesn't exist
        create_fact_table(cursor_dwh)

        # Fetch data from operational database treasure_log table
        treasure_log_query = """
        SELECT tl.id, tl.log_time, tl.hunter_id, tl.treasure_id, t.difficulty, t.terrain, c.city_name, c.city_id, t.owner_id
        FROM treasure_log tl
        JOIN Treasure t ON tl.treasure_id = t.id
        JOIN city c ON t.city_city_id = c.city_id
        WHERE tl.log_type = 2
        """
        cursor_op.execute(treasure_log_query)
        for row in cursor_op.fetchall():
            log_id, log_time, hunter_id, treasure_id, difficulty, terrain, city_name, city_id, owner_id = row

            # Check if the record already exists in the fact table
            factTreasureFound_query = "SELECT TREASURE_ID FROM FactTreasureFound WHERE TREASURE_ID = ?"
            cursor_dwh.execute(factTreasureFound_query, treasure_id)

            if not cursor_dwh.fetchone():  # If the record doesn't exist, we want to only insert SK
                day_SK = get_dim_day_sk(cursor_dwh, log_time)
                if day_SK is None:
                    continue

                dimTreasureType_SK = get_dim_treasure_type_sk(cursor_dwh, treasure_id)
                if dimTreasureType_SK is None:
                    continue

                dimUser_SK = get_dim_user_sk(cursor_dwh, hunter_id, log_time)
                if dimUser_SK is None:
                    continue

                # Fetch weather information from operational database weather_history
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

                rain_id = get_rain_id(cursor_dwh, weather_type)
                if rain_id is None:
                    continue

                total_caches = get_total_caches(cursor_op)
                if total_caches is None:
                    continue

                # Insert into FactTreasureFound
                insert_fact_table(cursor_dwh, treasure_id, day_SK, dimTreasureType_SK, dimUser_SK, rain_id, total_caches, log_time)

        conn_dwh.commit()
        cursor_dwh.close()
        # close connections
        conn_op.close()
        conn_dwh.close()

        print("Data insertion completed.")

    except pyodbc.Error as e:
        print(f"Error in main function: {e}")

# Call the main function
if __name__ == "__main__":
    main()














# import pyodbc









# from dwh import establish_connection
# from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
#
# def create_fact_table(cursor):
#     # Create the FactTreasureFound table if it doesn't exist
#     cursor.execute("""
#     IF NOT EXISTS (
#         SELECT 1
#         FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_NAME = 'FactTreasureFound'
#     )
#     BEGIN
#         CREATE TABLE FactTreasureFound (
#             TREASURE_ID INT PRIMARY KEY,
#             DIM_DAY_SK INT,
#             DIM_TREASURE_TYPE_SK INT,
#             DIM_USER_SK INT,
#             RAIN_ID INT,
#             TOTAL_CACHES INT,
#             CREATED_AT DATETIME
#         )
#     END
#     """)
#
# def main():
#     try:
#         # Create connection strings
#         conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
#         conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
#
#         # Create cursor objects
#         cursor_op = conn_op.cursor()
#         cursor_dwh = conn_dwh.cursor()
#
#         # Create the FactTreasureFound table if it doesn't exist
#         create_fact_table(cursor_dwh)
#
#         # Fetch data from operational database treasure_log table
#         treasure_log_query = """
#         SELECT tl.id, tl.log_time, tl.hunter_id, tl.treasure_id, t.difficulty, t.terrain, c.city_name, c.city_id, t.owner_id
#         FROM treasure_log tl
#         JOIN Treasure t ON tl.treasure_id = t.id
#         JOIN city c ON t.city_city_id = c.city_id
#         WHERE tl.log_type = 2
#         """
#         cursor_op.execute(treasure_log_query)
#         for row in cursor_op.fetchall():
#             log_id, log_time, hunter_id, treasure_id, difficulty, terrain, city_name, city_id, owner_id = row
#
#             # Check if the record already exists in the fact table
#             factTreasureFound_query = "SELECT TREASURE_ID FROM FactTreasureFound WHERE TREASURE_ID = ?"
#             cursor_dwh.execute(factTreasureFound_query, treasure_id)
#
#             if not cursor_dwh.fetchone():  # If the record doesn't exist, we want to only insert SK
#                 # Fetch DIM_Day_SK
#                 dimDay_query = "SELECT day_SK FROM dimDay WHERE Date = ?"
#                 cursor_dwh.execute(dimDay_query, log_time)
#                 day_SK = cursor_dwh.fetchone()
#
#                 if day_SK:
#                     day_SK = day_SK[0]
#                 else:
#                     # Handle the case when day_SK is not found
#                     print(f"Error: day_SK not found for date: {log_time}")
#                     continue
#
#                 # Fetch DIM_TreasureType_SK
#                 dimTreasureType_query = "SELECT treasureType_SK FROM dimTreasureType WHERE treasure_id = ?"
#                 cursor_dwh.execute(dimTreasureType_query, treasure_id)
#                 dimTreasureType_SK = cursor_dwh.fetchone()
#
#                 if dimTreasureType_SK:
#                     dimTreasureType_SK = dimTreasureType_SK[0]
#                 else:
#                     # Handle the case when dimTreasureType_SK is not found
#                     print(f"Error: dimTreasureType_SK not found for treasure_id: {treasure_id}")
#                     continue
#
#                 # Fetch DIM_User_SK (considering SCD attributes)
#                 dimUser_query = """
#                 SELECT user_SK
#                 FROM dimUser
#                 WHERE userId = ?
#                 AND scd_active = 1
#                 """
#                 cursor_dwh.execute(dimUser_query, hunter_id)
#                 dimUser_SK = cursor_dwh.fetchone()
#
#                 if dimUser_SK:
#                     dimUser_SK = dimUser_SK[0]
#                 else:
#                     # If user not found with scd_active = 1, check if there's a historical record
#                     historical_user_query = """
#                     SELECT user_SK
#                     FROM dimUser
#                     WHERE userId = ?
#                     AND scd_active = 0
#                     AND scd_end >= ?
#                     """
#                     cursor_dwh.execute(historical_user_query, (hunter_id, log_time))
#                     historical_user_SK = cursor_dwh.fetchone()
#
#                     if historical_user_SK:
#                         # Reactivate the historical user record
#                         reactivate_user_query = """
#                         UPDATE dimUser
#                         SET scd_active = 1,
#                             scd_end = NULL,
#                             scd_version = scd_version + 1
#                         WHERE user_SK = ?
#                         """
#                         cursor_dwh.execute(reactivate_user_query, historical_user_SK[0])
#                         dimUser_SK = historical_user_SK[0]
#                     else:
#                         # Handle the case when dimUser_SK is not found
#                         print(f"Error: dimUser_SK not found for hunter_id: {hunter_id}")
#                         continue
#
#                 # Fetch weather information from weather_history
#                 weather_query = """
#                 SELECT TOP 1 city, weather_code, weather_type
#                 FROM weather_history
#                 WHERE city = ? AND date <= ?
#                 ORDER BY date DESC
#                 """
#                 cursor_op.execute(weather_query, (city_name, log_time))
#                 weather_info = cursor_op.fetchone()
#
#                 if weather_info:
#                     city, weather_code, weather_type = weather_info
#                 else:
#                     # If weather info is not found, link to "Rain situation unknown"
#                     city, weather_code, weather_type = None, None, "Rain situation unknown"
#
#                 # Fetch rain_id from dimRain based on weather_type
#                 rain_id_query = """
#                 SELECT rain_id
#                 FROM dimRain
#                 WHERE rain_category = ?
#                 """
#                 cursor_dwh.execute(rain_id_query, weather_type)
#                 rain_id = cursor_dwh.fetchone()
#
#                 if rain_id:
#                     rain_id = rain_id[0]
#                 else:
#                     # Handle the case when rain_id is not found
#                     print(f"Error: rain_id not found for weather_type: {weather_type}")
#                     continue
#
#                 # Fetch TOTAL_CACHES from treasure_log
#                 total_caches_query = """
#                 SELECT COUNT(*)
#                 FROM treasure_log
#                 WHERE log_type = 2
#                 """
#                 cursor_op.execute(total_caches_query)
#                 total_caches = cursor_op.fetchone()[0]
#
#                 # Insert into FactTreasureFound
#                 insert_query = """
#                 INSERT INTO FactTreasureFound (TREASURE_ID, DIM_DAY_SK, DIM_TREASURE_TYPE_SK, DIM_USER_SK, RAIN_ID, TOTAL_CACHES, CREATED_AT)
#                 VALUES (?, ?, ?, ?, ?, ?, ?)
#                 """
#                 cursor_dwh.execute(insert_query,
#                                    (treasure_id, day_SK, dimTreasureType_SK, dimUser_SK, rain_id, total_caches, log_time))
#
#         conn_dwh.commit()
#         cursor_dwh.close()
#         # close connections
#         conn_op.close()
#         conn_dwh.close()
#
#         print("Data insertion completed.")
#
#     except pyodbc.Error as e:
#         print(f"Error in main function: {e}")
#
# # Call the main function
# if __name__ == "__main__":
#     main()
