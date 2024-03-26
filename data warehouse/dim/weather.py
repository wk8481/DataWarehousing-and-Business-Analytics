import pyodbc
from config import SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN

def create_dim_rain(cursor):
    """Creates the 'dimRain' table structure with rain categories."""

    try:
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dimRain')
        BEGIN
            CREATE TABLE dimRain (
                rain_id INT IDENTITY(1,1) PRIMARY KEY,
                rain_category VARCHAR(50) NOT NULL
            )

            INSERT INTO dimRain (rain_category) VALUES
            ('No Rain'),
            ('With Rain'),
            ('Unknown')
        END
        """)
        print("Table 'dimRain' created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating 'dimRain' table: {e}")

def main():
    try:
        # Establish connection to the data warehouse
        conn_dwh = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DSN={DSN};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE_DWH};')
        cursor_dwh = conn_dwh.cursor()

        # Create the dimRain table and insert sample data
        create_dim_rain(cursor_dwh)

        # Commit changes and close connection
        conn_dwh.commit()
        cursor_dwh.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        print(f"Database error: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
