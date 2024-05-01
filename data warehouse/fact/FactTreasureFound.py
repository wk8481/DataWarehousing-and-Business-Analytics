# import pyodbc
# import datetime
# from dwh import establish_connection
# from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
# from tqdm import tqdm
#
#
# def create_fact_table(cursor):
#     # Drop the FactTreasureFound table if it exists
#     cursor.execute("""
#     IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FactTreasureFound')
#     BEGIN
#         DROP TABLE FactTreasureFound;
#     END
#     """)
#
#     # Create the FactTreasureFound table without constraints
#     cursor.execute("""
#     CREATE TABLE FactTreasureFound (
#         TREASURE_ID INT PRIMARY KEY,
#         DIM_DAY_SK INT,
#         DIM_TREASURE_TYPE_SK INT,
#         DIM_USER_SK INT,
#         RAIN_ID INT,
#         TOTAL_CACHES INT,
#         CREATED_AT DATETIME,
#         DEFAULT_MEASUREMENT INT,
#         DURATION_OF_QUEST INT
#     );
#     """)
#
#
# def lookup_weather_info(cursor_op, city_name, log_time):
#     """
#     Look up weather information based on city name, log time (hour and day).
#     """
#     # Extract the date, day, and hour from the log_time
#     log_date = log_time.strftime('%Y-%m-%d')
#     log_day = log_time.strftime('%A')  # Extract the day of the week
#     log_hour = log_time.hour
#
#     # Query for weather data matching the specific date, day, and hour
#     weather_query = """
#     SELECT TOP 1 city, weather_code, weather_type
#     FROM weather_history
#     WHERE city = ? AND [date] = ? AND [day] = ? AND [hour] = ?
#     ORDER BY [date] DESC
#     """
#     cursor_op.execute(weather_query, (city_name, log_date, log_day, log_hour))
#     return cursor_op.fetchone()
#
#
# def lookup_dim_day_sk(cursor_dwh, log_time):
#     """
#     Look up DIM_DAY_SK based on log_time.
#     """
#     dimDate_query = "SELECT day_SK FROM dimDay WHERE [Date] = ?"
#     cursor_dwh.execute(dimDate_query, log_time.strftime('%Y-%m-%d'))
#     result = cursor_dwh.fetchone()
#     if result:
#         return result[0]  # Return DIM_DAY_SK if found
#     else:
#         return None  # Return None if not found
#
#
# def lookup_dim_user_sk(cursor_dwh, hunter_id):
#     """
#     Look up DIM_USER_SK based on hunter_id.
#     """
#     dimUser_query = "SELECT user_SK FROM dimUser WHERE [userId] = ? AND scd_active = 1"
#
#     cursor_dwh.execute(dimUser_query, hunter_id)
#     result = cursor_dwh.fetchone()
#     if result:
#         return result[0]  # Return DIM_USER_SK if found
#     else:
#         return None  # Return None if not found
#
#
# def lookup_dim_treasure_type_sk(cursor_dwh, difficulty, size, terrain, visibility):
#     """
#     Look up DIM_TREASURE_TYPE_SK based on the combination of difficulty, size, terrain, and visibility.
#     """
#     dimTreasure_query = """
#     SELECT treasureType_SK
#     FROM dimTreasureType
#     WHERE difficulty = ? AND size = ? AND terrain = ? AND visibility = ?
#     """
#     cursor_dwh.execute(dimTreasure_query, (difficulty, size, terrain, visibility))
#     result = cursor_dwh.fetchone()
#     if result:
#         return result[0]  # Return DIM_TREASURE_TYPE_SK if found
#     else:
#         return None  # Return None if not found
#
#
# def lookup_rain_id(cursor_dwh, weather_type):
#     """
#     Look up RAIN_ID based on weather_type.
#     """
#     rain_id_query = "SELECT rain_id FROM dimRain WHERE rain_category = ?"
#     cursor_dwh.execute(rain_id_query, weather_type)
#     result = cursor_dwh.fetchone()
#     if result:
#         return result[0]  # Return RAIN_ID if found
#     else:
#         return None  # Return None if not found
#
#
# # Define other lookup functions...
#
# # Update insert function to use top cities for weather lookup
# def insert(cursor_op, cursor_dwh):
#     # Fetch data from operational database treasure_log table
#     treasure_log_query = """
# SELECT
#     tl.id AS log_id,
#     DATEADD(HOUR, DATEDIFF(HOUR, 0, tl.log_time), 0) AS rounded_hour,
#     CONVERT(DATE, DATEADD(HOUR, DATEDIFF(HOUR, 0, tl.log_time), 0)) AS rounded_day,
#     tl.hunter_id AS user_id,
#     tl.treasure_id,
#     tl.session_start,
#     tr.difficulty,
#     st.container_size,
#     tr.terrain,
#     st.visibility,
#     wh.city AS weather_city,
#     wh.weather_type
# FROM
#     treasure_log tl
# INNER JOIN
#     treasure tr ON tl.treasure_id = tr.id
# INNER JOIN
#     treasure_stages ts ON tr.id = ts.treasure_id
# INNER JOIN
#     stage st ON ts.stages_id = st.id
# INNER JOIN
#     weather_history wh ON DATEADD(HOUR, DATEDIFF(HOUR, 0, tl.log_time), 0) = wh.[date] AND DATEPART(HOUR, tl.log_time) = wh.[hour]
# WHERE
#     tl.log_type = 2
# """
#
#     cursor_op.execute(treasure_log_query)
#
#     # Get total number of rows for treasure log for progress bar
#     total_treasure_rows = cursor_op.rowcount
#
#     # Initialize tqdm progress bar for treasure log
#     progress_bar_treasure = tqdm(total=total_treasure_rows, desc="Inserting treasure log data")
#
#     for row in cursor_op.fetchall():
#         log_id, rounded_hour, rounded_day, user_id, treasure_id, session_start, difficulty, size, terrain, visibility, weather_city, weather_type = row
#
#         # Check if the record already exists in the fact table
#         factTreasureFound_query = "SELECT TREASURE_ID FROM FactTreasureFound WHERE TREASURE_ID = ?"
#         cursor_dwh.execute(factTreasureFound_query, treasure_id)
#
#         if not cursor_dwh.fetchone():  # If the record doesn't exist, we want to only insert SK
#             # Lookup DIM_DAY_SK
#             dimDate_SK = lookup_dim_day_sk(cursor_dwh, log_time)
#
#             # Lookup DIM_USER_SK
#             dimUser_SK = lookup_dim_user_sk(cursor_dwh, user_id)
#
#             # Lookup weather info based on city name and log time
#             weather_info = lookup_weather_info(cursor_op, weather_city, log_time)
#
#             # Lookup RAIN_ID from dimRain based on weather_type
#             rain_id = lookup_rain_id(cursor_dwh, weather_info[2])
#
#             # Lookup treasure type SK based on difficulty, size, terrain, and visibility
#             treasure_type_sk = lookup_dim_treasure_type_sk(cursor_dwh, difficulty, size, terrain, visibility)
#
#             # Lookup TOTAL_CACHES from treasure_log
#             total_caches_query = "SELECT COUNT(*) FROM treasure_log WHERE log_type = 2"
#             cursor_op.execute(total_caches_query)
#             total_caches = cursor_op.fetchone()[0]
#
#             # Calculate Duration of the Quest
#             duration_of_quest = (session_start - log_time).total_seconds() if session_start else None
#
#             # Insert into FactTreasureFound for treasure log
#             insert_treasure_query = """
#             INSERT INTO FactTreasureFound (TREASURE_ID, DIM_DAY_SK, DIM_TREASURE_TYPE_SK, DIM_USER_SK, RAIN_ID, TOTAL_CACHES, CREATED_AT, DEFAULT_MEASUREMENT, DURATION_OF_QUEST)
#             VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)  -- Set DEFAULT_MEASUREMENT to 1
#             """
#             cursor_dwh.execute(insert_treasure_query,
#                                (treasure_id, dimDate_SK, treasure_type_sk, dimUser_SK, rain_id, total_caches, log_time,
#                                 duration_of_quest))
#
#         # Update progress bar for treasure log
#         progress_bar_treasure.update(1)
#
#     # Close progress bar for treasure log
#     progress_bar_treasure.close()
#
#     # Commit the transaction
#     cursor_dwh.commit()
#
#
# def job_fill_fact_table(cursor_op, cursor_dwh):
#     try:
#         # Empty the FactTreasureFound table
#         cursor_dwh.execute("DELETE FROM FactTreasureFound")
#         cursor_dwh.commit()
#
#         # Fill the FactTreasureFound table
#         insert(cursor_op, cursor_dwh)
#         print("FactTreasureFound table filled.")
#
#     except pyodbc.Error as e:
#         print(f"Error in job_fill_fact_table: {e}")
#
#
# def main():
#     global cursor_op, cursor_dwh, conn_op, conn_dwh
#     try:
#         conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
#         conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
#
#         cursor_op = conn_op.cursor()
#         cursor_dwh = conn_dwh.cursor()
#
#         # Create the FactTreasureFound table if it doesn't exist
#         create_fact_table(cursor_dwh)
#
#         # Run the job to empty and fill the FactTreasureFound table
#         job_fill_fact_table(cursor_op, cursor_dwh)
#
#     except pyodbc.Error as e:
#         print(f"Error in main function: {e}")
#
#     finally:
#         if cursor_op:
#             cursor_op.close()
#         if cursor_dwh:
#             cursor_dwh.close()
#         if conn_op:
#             conn_op.close()
#         if conn_dwh:
#             conn_dwh.close()
#
#         print("Data insertion completed.")
#
#
# if __name__ == "__main__":
#     main()


import logging

import pandas as pd
import pyodbc

import dwh as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER

logging.basicConfig(level=logging.INFO)


def create_table(cursor_dwh):
    """
    Create the 'factTreasureFound' table in the data warehouse.
    :param cursor_dwh: Data warehouse cursor object
    :return: None
    """
    try:
        # Drop the table if it exists
        cursor_dwh.execute("""
        IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'factTreasureFound')
        BEGIN
            DROP TABLE catchem_dwh.dbo.factTreasureFound;
        END
        """)

        # Create the table
        cursor_dwh.execute("""
          CREATE TABLE catchem_dwh.dbo.factTreasureFound
(
    TreasureFoundID INT IDENTITY (1,1) PRIMARY KEY,
    DIM_DAY_SK INT,
    DIM_TREASURE_TYPE_SK INT,
    DIM_USER_SK INT,
    RAIN_ID INT,
    Duration        INT,
    CreationDate    DATETIME2 DEFAULT GETDATE(),
    Constant        BIT,
    FOREIGN KEY (DIM_USER_SK) REFERENCES dbo.dimUser (user_SK),
    FOREIGN KEY (DIM_TREASURE_TYPE_SK) REFERENCES dbo.dimTreasureType (treasureType_SK),
    FOREIGN KEY (DIM_DAY_SK) REFERENCES dbo.dimDay (day_SK),
    FOREIGN KEY (RAIN_ID) REFERENCES dbo.dimRain (RAIN_ID)
)

        """)
        logging.info("factTreasureFound table created successfully.")
    except pyodbc.Error as e:
        logging.error(f"Error creating factTreasureFound table: {e}")

def fetch_treasure_log_data(cursor_op):
    """
    Fetch data from the 'treasure_log' table.
    :param cursor_op: operational database cursor object
    :return: DataFrame containing data from the 'treasure_log' table
    """
    try:
        # Fetch data from the 'treasure_log' table
        cursor_op.execute("""
            SELECT id, log_time, hunter_id, treasure_id, session_start
            FROM catchem_9_2023.dbo.treasure_log
        """)

        # Convert the data to a DataFrame
        treasure_log_rows = [tuple(row) for row in cursor_op.fetchall()]
        columns = ['id', 'log_time', 'hunter_id', 'treasure_id', 'session_start']
        df_treasure_log = pd.DataFrame(treasure_log_rows, columns=columns)

        return df_treasure_log

    except pyodbc.Error as e:
        logging.error(f"Error fetching data from treasure_log table: {e}")
        return pd.DataFrame()


def populate_fact_treasure_found(cursor_dwh, treasure_log_data):
    """
    Populate the 'factTreasureFound' table.
    :param cursor_dwh: data warehouse cursor object
    :param treasure_log_data: DataFrame containing data from the 'treasure_log' table
    :return: None
    """
    try:
        for index, row in treasure_log_data.iterrows():
            # Get user_ids from dimUser table for the given hunter_id
            cursor_dwh.execute("""
                SELECT user_SK
                FROM catchem_dwh.dbo.dimUser
                WHERE userId = ?
            """, (row['hunter_id'],))
            user_rows = cursor_dwh.fetchall()

            if user_rows:
                for user_row in user_rows:
                    if user_row:
                        user_sk = user_row[0]
                    else:
                        continue

                    # Get TreasureTypeID from 'dimTreasureType' table
                    cursor_dwh.execute("""
                        SELECT tt.treasureType_SK
                        FROM catchem_9_2023.dbo.treasure AS t
                        JOIN catchem_9_2023.dbo.treasure_stages AS ts ON t.id = ts.treasure_id
                        JOIN catchem_9_2023.dbo.stage AS s ON ts.stages_id = s.id
                        JOIN catchem_dwh.dbo.dimTreasureType AS tt ON t.difficulty = tt.difficulty
                            AND t.terrain = tt.terrain
                            AND (SELECT COUNT(sts.stages_id)
                                 FROM catchem_9_2023.dbo.treasure_stages AS sts
                                 WHERE sts.treasure_id = t.id) = tt.size
                            AND (SELECT MAX(st.visibility)
                                 FROM catchem_9_2023.dbo.treasure_stages AS sts
                                      JOIN catchem_9_2023.dbo.stage AS st ON sts.stages_id = st.id
                                 WHERE sts.treasure_id = t.id) = tt.visibility
                        WHERE t.id = ?;
                    """, (row['treasure_id'],))
                    treasure_type_row = cursor_dwh.fetchone()
                    if treasure_type_row:
                        treasure_type_sk = treasure_type_row[0]
                    else:
                        continue

                    # Get DateID from 'dimDate' table
                    cursor_dwh.execute("""
                        SELECT day_SK
                        FROM catchem_dwh.dbo.dimDay
                        WHERE CAST(Date AS DATE) = CAST(? AS DATE)
                    """, (row['session_start'],))
                    date_row = cursor_dwh.fetchone()
                    if date_row:
                        date_sk = date_row[0]
                    else:
                        continue

                    # Get WeatherId from 'dimWeather' table
                    cursor_dwh.execute("""
                        SELECT dw.rain_id
                        FROM catchem_dwh.dbo.dimRain AS dw
                        JOIN catchem_9_2023.dbo.weather_history AS wh ON dw.rain_category = wh.weather_type
                        WHERE DATEPART(HOUR, wh.hour) = DATEPART(HOUR, ?);
                    """, (row['log_time'],))
                    weather_row = cursor_dwh.fetchone()
                    if weather_row:
                        weather_sk = weather_row[0]
                    else:
                        continue

                    logging.info(f"Log Time: {row['log_time']}")

                    # Insert data into 'factTreasureFound' table
                    try:
                        duration = (row['log_time'] - row['session_start']).total_seconds()
                        cursor_dwh.execute("""
                           INSERT INTO catchem_dwh.dbo.factTreasureFound(DIM_USER_SK, DIM_TREASURE_TYPE_SK, DIM_DAY_SK, RAIN_ID, Duration, CreationDate, Constant)
VALUES (?, ?, ?, ?, ?, GETDATE(), 1)

                        """, (user_sk, treasure_type_sk, date_sk, weather_sk, int(duration)))
                        logging.info(f"Inserted data into factTreasureFound table for hunter_id: {row['hunter_id']}")
                        cursor_dwh.commit()

                    except pyodbc.Error as e:
                        logging.error(f"Error inserting data into factTreasureFound table: {e}")

            else:
                continue

    except pyodbc.Error as e:
        logging.error(f"Error populating fact_treasure_found table: {e}")


def empty_fact_treasure_found(cursor_dwh):
    """
    Empty the 'factTreasureFound' table.
    :param cursor_dwh: data warehouse cursor object
    :return: None
    """
    try:
        cursor_dwh.execute("TRUNCATE TABLE catchem_dwh.dbo.factTreasureFound")
        cursor_dwh.commit()
        logging.info("The 'factTreasureFound' table has been successfully emptied.")
    except pyodbc.Error as e:
        logging.error("Error encountered while emptying the 'factTreasureFound' table: Unable to truncate table.")


def main():
    """
    Main function.
    """
    try:
        # Establish connections to the databases
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        cursor_dwh = conn_dwh.cursor()
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        cursor_op = conn_op.cursor()

        # Create the 'factTreasureFound' table
        create_table(cursor_dwh)
        print(f"create fact table successfully")

        # Empty the 'factTreasureFound' table
        empty_fact_treasure_found(cursor_dwh)

        # Fetch data from the 'treasure_log' table
        treasure_log_data = fetch_treasure_log_data(cursor_op)

        # Populate the 'factTreasureFound' table
        if not treasure_log_data.empty:
            populate_fact_treasure_found(cursor_dwh, treasure_log_data)
        else:
            logging.warning("No data fetched from treasure_log table, skipping population.")

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        logging.error(f"Error connecting to the database: {e}")


if __name__ == "__main__":
    main()
