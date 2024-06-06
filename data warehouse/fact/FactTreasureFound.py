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
