import pandas as pd
import pyodbc
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER

def fetch_min_order_date(cursor_op):
    """
    Fetches the minimum order date from the 'sales' table.
    Args:
        cursor_op: The cursor object for the 'tutorial_op' database.
    Returns:
        str: The minimum order date.
    """
    cursor_op.execute('SELECT MIN(Order_date) FROM tutorial_op.dbo.sales')
    return cursor_op.fetchone()[0]

def fill_table_dim_date(cursor_dwh, start_date, end_date='2040-01-01', table_name='dimDay'):
    """
    Fills the 'dimDay' table with date-related data.
    Args:
        cursor_dwh: The cursor object for the 'tutorial_dwh' database.
        start_date (str): The start date for filling the table.
        end_date (str): The end date for filling the table (default is '2040-01-01').
        table_name (str): The name of the table (default is 'dimDay').
    """
    insert_query = f"""
    INSERT INTO tutorial_dwh.dbo.{table_name} ([Date], [DayOfMonth], [Month], [Year], [DayOfWeek], [DayOfYear], [Weekday], [MonthName], [Quarter])
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
        quarter = (current_date.month - 1) // 3 + 1  # Calculate quarter based on the month

        # Execute the INSERT query
        cursor_dwh.execute(insert_query, (
            current_date, day_of_month, month, year, day_of_week, day_of_year, weekday, month_name, quarter
        ))

        # Commit the transaction
        cursor_dwh.commit()
        current_date += pd.Timedelta(days=1)

def main():
    try:
        # Define connection parameters
        # Connect to the 'tutorial_op' database
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD,DRIVER)
        cursor_op = conn_op.cursor()

        # Connect to the 'tutorial_dwh' database
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD,DRIVER)
        cursor_dwh = conn_dwh.cursor()

        # Fetch minimum order date
        start_date = fetch_min_order_date(cursor_op)
        print(start_date)

        # Fill the 'dimDay' table
        fill_table_dim_date(cursor_dwh, start_date, '2100-01-01', 'dimDay')

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()
