import pyodbc
import pandas as pd
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from datetime import datetime

def fill_table_dim_date_test(cursor_dwh, start_date, end_date='2040-01-01', table_name='dimDay'):
    """
    Fills the 'dimDay' table with date-related date if the table doesn't exist, otherwise creates it.
    Args:
        cursor_dwh (pyodbc.Cursor): The cursor object for the 'catchem_dwh' database.
        start_date(str): The start date for filling the table.
        end_date(str, optional): The end date for filling the table (default is '2040-01-01').
        table_name (str, optional): The name of the table (default is 'dimDay').

    """
 # Check if the table exists
    try:
        cursor_dwh.execute(f"SELECT TOP 1 * FROM {table_name}")
    except pyodbc.Error as e:
        if "Invalid object name" in str(e):  # Check for "Invalid object name" error
            print(f"Table '{table_name}' does not exist. Creating it...")

            # Create the table with appropriate data types
            create_table_query = f"""
            CREATE TABLE {table_name} (
                [Date] date PRIMARY KEY,
                [DayOfMonth] int,
                [Month] int,
                [Year] int,
                [DayOfWeek] int,
                [DayOfYear] int,
                [Weekday] nvarchar(10),
                [MonthName] nvarchar(20),
                [Season] nvarchar(10)
            )
            """
            cursor_dwh.execute(create_table_query)
            cursor_dwh.commit()
            print(f"Table '{table_name}' created successfully.")



def fetch_min_log_time(cursor_op):
    """
    Fetches the minimum log time from the 'treasure_log' table where log_type is 2 (treasureFound).
    Args:
        cursor_op: The cursor object for the 'catchem' database.
    Returns:
        str: The minimum log time.
    """
    cursor_op.execute("SELECT MIN(log_time) FROM treasure_log WHERE log_type = 2")
    return cursor_op.fetchone()[0]

def fill_table_dim_date(cursor_dwh, start_date, end_date='2040-01-01', table_name='dimDay'):
    """
    Fills the 'dimDay' table with date-related data.
    Args:
        cursor_dwh: The cursor object for the 'catchem_dwh' database.
        start_date (str): The start date for filling the table.
        end_date (str): The end date for filling the table (default is '2040-01-01').
        table_name (str): The name of the table (default is 'dimDay').
    """
    insert_query = f"""
    INSERT INTO {table_name} ([Date], [DayOfMonth], [Month], [Year], [DayOfWeek], [DayOfYear], [Weekday], [MonthName], [Season])
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    current_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    while current_date <= end_date:
        day_of_month = current_date.day
        month = current_date.month
        year = current_date.year
        day_of_week = current_date.dayofweek
        day_of_year = current_date.timetuple().tm_yday
        weekday = current_date.strftime('%A')
        month_name = current_date.strftime('%B')
        season = get_season(current_date)  # Get the season based on the date

        # Execute the INSERT query
        cursor_dwh.execute(insert_query, (
            current_date, day_of_month, month, year, day_of_week, day_of_year, weekday, month_name, season
        ))

        # Commit the transaction
        cursor_dwh.commit()
        current_date += pd.Timedelta(days=1)



def get_season(date):
    """
    Returns the season based on the provided date.
    Args:
        date (datetime): The date for which to determine the season.
    Returns:
        str: The season (Spring, Summer, Autumn, Winter).
    """
    month = date.month
    if 3 <= month <= 5:
        return 'Spring'
    elif 6 <= month <= 8:
        return 'Summer'
    elif 9 <= month <= 11:
        return 'Autumn'
    else:
        return 'Winter'

def main():
    try:
        # Connect to the 'catchem' database
        conn_op = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_OP};')
        cursor_op = conn_op.cursor()

        # Connect to the 'catchem_dwh' database
        conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
        cursor_dwh = conn_dwh.cursor()



        # Fetch minimum log time where log_type is 'treasureFound'
        start_time = fetch_min_log_time(cursor_op)
        print(f"Minimum Log Time: {start_time}")

        # # Fill table dimDaytest
        # fill_table_dim_date_test(cursor_dwh, start_time, '2100-01-01', 'dimDaytest')

        #fill_table_dim_date_test(cursor_dwh, start_time, '2100-01-01', table_name='dimDay')

        # Fill the 'dimDay' table
        fill_table_dim_date(cursor_dwh, start_time, '2100-01-01', 'dimDay')

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()
