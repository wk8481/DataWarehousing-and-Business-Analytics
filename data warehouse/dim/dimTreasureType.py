import pyodbc
import pandas as pd

from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from dwh import establish_connection

def create_dim_treasure_type_table(conn):
    """
    Create "dimTreasureType" table if it doesn't exist in the data warehouse
    arg: (pyodbc.Connection): Database connection object
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dimTreasureType')
        BEGIN
            CREATE TABLE dimTreasureType (
                treasureType_SK INT IDENTITY(1,1) PRIMARY KEY,
                difficulty INT NOT NULL,
                terrain INT NOT NULL,
                size INT NOT NULL,
                visibility INT NULL
            )
        END
        """)
        cursor.commit()
        print("Table 'dimTreasureType' created successfully or already exists")
    except pyodbc.Error as e:
        print(f"Error creating table 'dimTreasureType': {e}")

def fill_dim_treasure_type_table(cursor):
    """
    Fills the 'dimTreasureType' table with all possible combinations.
    Args:
        cursor (pyodbc.Cursor): Cursor object for executing SQL commands.
    """
    try:
        for difficulty in range(5):  # Assuming 0-4 range for difficulty
            for terrain in range(5):  # Assuming 0-4 range for terrain
                for size in range(1, 4):  # Assuming 1-3 range for size
                    for visibility in range(3):  # Assuming 0-2 range for visibility
                        insert_query = """
                        INSERT INTO dimTreasureType (difficulty, terrain, size, visibility)
                        VALUES (?, ?, ?, ?)
                        """
                        cursor.execute(insert_query, (difficulty, terrain, size, visibility))

        cursor.commit()
        print("Treasure types inserted into 'dimTreasureType' table successfully.")
    except pyodbc.Error as e:
        print(f"Error inserting into 'dimTreasureType' table: {e}")

def analyze_execution_plan(cursor, query):
    """
    Analyze the execution plan of a given query using SET STATISTICS PROFILE ON.
    Args:
        cursor (pyodbc.Cursor): Cursor object for executing SQL commands.
        query (str): SQL query to be analyzed.
    """
    try:
        # Enable execution plan statistics
        cursor.execute("SET STATISTICS PROFILE ON")

        # Execute the query
        cursor.execute(query)

        # Fetch and print the execution plan
        print("Execution Plan:")
        for row in cursor.fetchall():
            print(row)

        # Disable execution plan statistics
        cursor.execute("SET STATISTICS PROFILE OFF")
    except pyodbc.Error as e:
        print(f"Error analyzing execution plan: {e}")



def create_indexed_view(cursor):
    try:
        # Step 1: Verify SET options
        cursor.execute("SET NUMERIC_ROUNDABORT OFF")
        cursor.execute("SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON")

        # Step 2: Create the view with SCHEMABINDING
        cursor.execute("""
            CREATE VIEW vw_DimTreasureType_Indexed
AS
SELECT 
    difficulty,
    terrain,
    COUNT_BIG(*) AS size,
    SUM(CAST(visibility AS FLOAT)) AS total_visibility
FROM 
    dbo.dimTreasureType
GROUP BY 
    difficulty, terrain;

        """)

        # Step 3: Create the unique clustered index on the view
        cursor.execute("""
            CREATE UNIQUE CLUSTERED INDEX IX_MyIndexedView 
            ON dbo.MyIndexedView (difficulty, terrain)
        """)

        print("Indexed view created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating indexed view: {e}")

def main():
    conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

    if conn_dwh:
        cursor = conn_dwh.cursor()

        # Step 1: Create dimTreasureType table
        create_dim_treasure_type_table(conn_dwh)

        # Step 2: Fill dimTreasureType table with all possible combinations
        fill_dim_treasure_type_table(cursor)

        # Step 3: Analyze the original execution plan
        sample_query = "SELECT * FROM dimTreasureType WHERE difficulty = 0 AND terrain = 0"
        analyze_execution_plan(cursor, sample_query)

        # Step 4: Create the indexed view
        create_indexed_view(cursor)

        # Step 5: Analyze execution plan after implementing the indexed view
        analyze_execution_plan(cursor, sample_query)

        # Step 6: Decide whether to implement the indexed view (manual decision)

        # Close cursor and connection
        cursor.close()
        conn_dwh.close()

if __name__ == "__main__":
    main()
