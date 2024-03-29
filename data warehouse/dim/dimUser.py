
# # get city name and country name for each user by using cityId from user, countryCode
# # Experience level : get number of caches, log type [message=0], [not found=1] [found=2] , hunter_id = user_id in user table
# # definition of experience level
# # Starter: No 'Found' logs posted yet
# # Amateur: Fewer than four 'Found' logs posted
# # Professional: 4-10 'Found' logs posted
# # Pirate: More than 10 found logs posted
# # Dedicator or not : if when owner_id = user_Id in treasure table, he is a dedicator

import datetime
from datetime import datetime
import pyodbc
import pandas as pd

from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from dwh import establish_connection

# Define query to extract user attribute
user_query = """
SELECT
    u.id,
    u.first_name,
    u.last_name,
    c.city_name,
    co.name,
    CASE
        WHEN l.found_logs IS NULL THEN 'Starter'
        WHEN l.found_logs < 4 THEN 'Amateur'
        WHEN l.found_logs BETWEEN 4 AND 10 THEN 'Professional'
        ELSE 'Pirate'
    END AS experiece_level,
    CASE
        WHEN t.owner_id IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS dedicator
FROM
    user_table u
LEFT JOIN
    city c ON u.city_city_id = c.city_id
LEFT JOIN
    country co ON c.country_code = co.code
LEFT JOIN (
    SELECT
        hunter_id,
        SUM(CASE WHEN log_type = 2 THEN 1 ELSE 0 END) AS found_logs
    FROM
        treasure_log
    WHERE
        log_Type = 2
    GROUP BY
        hunter_id
) l ON u.id=l.hunter_id
LEFT JOIN
    treasure t ON u.id = t.owner_id   
"""

# Function to create dimUser table if it doesn't exist
def create_dimUser_table(conn):
    cursor = conn.cursor()

    # SQL query to create dimUser table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dimUser')
    BEGIN
        CREATE TABLE dimUser (
            user_SK INT IDENTITY(1,1) NOT NULL,
            userId INT NOT NULL,    
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            city_name VARCHAR(255) NOT NULL,
            country_name VARCHAR(255) NOT NULL,
            experience_level VARCHAR(50) NOT NULL,
            dedicator VARCHAR(3) NOT NULL,
            scd_start DATETIME NOT NULL,
            scd_end DATETIME,
            scd_version INT,
            scd_active BIT
        );
    END
    """

    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("dimUser table created successfully or already exists")
    except pyodbc.Error as e:
        print(f"Error creating dimUser table: {e}")
    finally:
        cursor.close()

# Function to handle SCD Type 2 updates for dimUser table
def handle_dimUser_scd(conn_op, conn_dwh):
    # these code added to check if 'cursor' is a cursor object
    # since we got AttributeError:: 'pyodbc.Cursor' object has no attribute 'cursor'

    if isinstance(conn_op, pyodbc.Cursor):
        cursor_op = conn_op
    else:
        cursor_op = conn_op.cursor()

    if isinstance(conn_dwh, pyodbc.Connection):
        cursor_dwh = conn_dwh.cursor()
    else:
        cursor_dwh = conn_dwh

    try:
        # Execute the user_query
        print("Extracting user data from chachem db...")
        cursor_op.execute(user_query)
        print("Query execution complete")
        rows = cursor_op.fetchall()

        for row in rows:
            # Extract data from the source table
            user_id, first_name, last_name, city_name, country_name, experience_level, dedicator = row

            # Execute query to check SCD Type 2 for userId
            cursor_dwh.execute("SELECT userId, scd_version FROM dimUser WHERE userId = ? AND scd_active = 1",
                               user_id)
            dim_rows = cursor_dwh.fetchall()

            if not dim_rows:
                # Insert a new record into dimUser if no record exists for the userId
                insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
                                                        experience_level, dedicator, scd_start, scd_end, scd_version,
                                                        scd_active)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                cursor_dwh.execute(insert_query, (user_id, first_name, last_name, city_name, country_name,
                                                   experience_level, dedicator, datetime.now(),
                                                   '', 1, 1))
            else:
                # Get the latest version and attributes
                latest_version = max(dim_rows, key=lambda x: x[1])[1]

                # Update the SCD attributes of the latest version if any changes
                update_query = """UPDATE dimUser SET scd_end = ?, scd_version = ?, scd_active = ?
                                  WHERE userId = ? AND scd_active = 1"""
                cursor_dwh.execute(update_query, (datetime.now(), latest_version, 0, user_id))

                # Insert a new record into dimUser with a new version
                insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
                                                      experience_level, dedicator, scd_start, scd_end, scd_version,
                                                      scd_active)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                cursor_dwh.execute(insert_query, (user_id, first_name, last_name, city_name, country_name,
                                                   experience_level, dedicator, datetime.now(),
                                                   '', latest_version + 1, 1))

        # Commit the transaction for all rows
        conn_dwh.commit()
        print("SCD Type 2 handling for dimUser completed successfully")

    except pyodbc.Error as e:
        print(f"Error handling SCD Type 2 for dimUser: {e}")
    finally:
        if not isinstance(conn_op, pyodbc.Cursor):
            cursor_op.close()
        if not isinstance(conn_dwh, pyodbc.Cursor):
            cursor_dwh.close()

# Function to establish connections and call the necessary functions
def main():
    try:
        # Connect to the 'catchem' database
        conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        cursor_op = conn_op.cursor()

        # Connect to the 'catchem_dwh' database
        conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        cursor_dwh = conn_dwh.cursor()

        # Create dimUser table if not exists
        create_dimUser_table(conn_dwh)

        # Handle SCD Type 2 updates for dimUser
        handle_dimUser_scd(cursor_op, cursor_dwh)

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

    finally:
        # added Try & except to avoid AttributeError

        try:
            cursor_op.close()
        except:
            pass
        try:
            cursor_dwh.close()
        except:
            pass
        try:
            conn_op.close()
        except:
            pass
        try:
            conn_dwh.close()
        except:
            pass

        # this causes AttributeError. If an object is None, calling method on it will result in AttributeError
        # if cursor_op and cursor_dwh:
        #     cursor_op.close()
        #     cursor_dwh.close()
        #
        # if conn_op and conn_dwh:
        #     conn_op.close()
        #     conn_dwh.close()

if __name__ == "__main__":
        main()

