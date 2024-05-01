
from datetime import datetime
import pyodbc
import pandas as pd

from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from dwh import establish_connection

# Define query to extract user attribute
# user_query = """
# SELECT
#     u.id,
#     u.first_name,
#     u.last_name,
#     u.number,
#     u.street,
#     c.city_name,
#     co.name as country_name,
#     COUNT(tl.id) as found_logs_no,
#     MAX(CASE WHEN t.owner_id IS NOT NULL THEN 'Yes' ELSE 'No' END) AS is_dedicator,
#     MIN(tl.log_time) as earliest_log_date
# FROM    user_table u
# LEFT JOIN    city c ON u.city_city_id = c.city_id
# LEFT JOIN    country co ON c.country_code = co.code
# LEFT JOIN    treasure_log tl ON u.id = tl.hunter_id
# LEFT JOIN    treasure t ON u.id = t.owner_id
# GROUP BY u.id, u.first_name, u.last_name, u.number, u.street, c.city_name, co.name
# """


user_query = """
SELECT
    userId, first_name, last_name,
    CONCAT(number,' ', street,' ', city_name,' ', country_name) AS address,
    found_logs_no,
    earliest_log_date,
    experience_level,
    is_dedicator
FROM
    (SELECT
        u.id AS userId,
        u.first_name,
        u.last_name,
        u.number,
        u.street,
        c.city_name,
        co.name AS country_name,
        COUNT(tl.id) AS found_logs_no,
        MIN(tl.log_time) AS earliest_log_date,
        CASE
            WHEN COUNT(tl.id) = 0 THEN 'Starter'
            WHEN COUNT(tl.id) < 4 THEN 'Amateur'
            WHEN COUNT(tl.id) BETWEEN 4 AND 10 THEN 'Professional'
            ELSE 'Pirate'
        END AS experience_level,
        MAX(CASE WHEN t.owner_id IS NOT NULL THEN 'Yes' ELSE 'No' END) AS is_dedicator
    FROM
        catchem_9_2023.dbo.user_table u
    LEFT JOIN
        catchem_9_2023.dbo.city c ON u.city_city_id = c.city_id
    LEFT JOIN
        catchem_9_2023.dbo.country co ON c.country_code = co.code
    LEFT JOIN
        catchem_9_2023.dbo.treasure_log tl ON u.id = tl.hunter_id
    LEFT JOIN
        catchem_9_2023.dbo.treasure t ON u.id = t.owner_id
    WHERE
        tl.log_type = 2
    GROUP BY
        u.id, u.first_name, u.last_name, u.number, u.street, c.city_name, co.name) AS subquery;
"""

# Function to create dimUser table if it doesn't exist
def create_dimUser_table(conn):
    cursor = conn.cursor()

    # SQL query to create dimUser table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dimUser')
    BEGIN
        CREATE TABLE dimUser (
            user_SK INT IDENTITY(1,1) PRIMARY KEY,
            userId BINARY(16) NOT NULL,    
            first_name NVARCHAR(255) NOT NULL,
            last_name NVARCHAR(255) NOT NULL,
            address NVARCHAR(MAX) NOT NULL,
            experience_level NVARCHAR(50),
            is_dedicator NVARCHAR(3),
            scd_start DATETIME,
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

# make sure is dedicator and address should be correct one for first run
def insert_first_run_data(cursor_op, cursor_dwh):
    # Execute the user_query
    print("Extracting user data from cachem db...")
    cursor_op.execute(user_query)
    print("Query execution complete")
    rows = cursor_op.fetchall()

    for row in rows:
        userId, first_name, last_name, address, found_logs_no, earliest_log_date, experience_level, is_dedicator  = row

        # Everyone is 'Starter' for first run
        experience_level = 'Starter'

        # Everyone is set as No for first run
        is_dedicator = 'No'

        # Set SCD date as null for first run
        scd_start = earliest_log_date
        scd_end = '2040-01-01'  # set far in the future

        # Insert record in the data warehouse
        insert_query = """
            INSERT INTO dimUser (userId, first_name, last_name, address,
             experience_level, is_dedicator, scd_start, scd_end, scd_version, scd_active)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """
        cursor_dwh.execute(insert_query, (userId, first_name, last_name, address,
                                          experience_level, is_dedicator, scd_start, scd_end, 1, 1))

        cursor_dwh.commit()
        print("Initial insert for dimUser completed successfully")


def handle_dimUser_scd(cursor_op, cursor_dwh):
    # Execute the user_query
    print("Extracting user data from catchem db...")
    cursor_op.execute(user_query)
    print("Query execution complete")
    rows = cursor_op.fetchall()

    for row in rows:
        # Extract data from the source table
        (userId, first_name, last_name, address,
         found_logs_no, earliest_log_date, experience_level, is_dedicator) = row

        # if user exist in dimUser already or not
        cursor_dwh.execute("SELECT * FROM dimUser WHERE userId = ?", (userId,))
        existing_user = cursor_dwh.fetchone()

        # cursor_dwh.execute("SELECT COUNT(*) FROM dimUser WHERE userId = ?", (userId,))
        # count = cursor_dwh.fetchone()[0]

        if existing_user is None:   # if this user doesn't exist, insert it
            cursor_dwh.execute("""INSERT INTO dimUser (userId, first_name, last_name, address,
                         experience_level, is_dedicator, scd_start, scd_end, scd_version, scd_active)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""", (userId, first_name, last_name, address, experience_level,
                                                                                 is_dedicator, datetime.now(), '2040-01-01', 1,1))
            print(f"Inserted new user into dimUser", userId)

        else: # user already exist, check for changes
            existing_address = existing_user[4]
            existing_is_dedicator = existing_user[6]

            if address != existing_address or is_dedicator != existing_is_dedicator:
                # update existing records and insert new version
                cursor_dwh.execute("""UPDATE dimUser SET scd_end = ?, scd_active =0 
                                        WHERE userId = ? AND scd_active = 1""", (datetime.now(), userId,))

                cursor_dwh.execute("""INSERT INTO dimUser (userId, first_name, last_name, address,
                             experience_level, is_dedicator, scd_start, scd_end, scd_version, scd_active)
                            VALUES (?,?,?,?,?,?,?,?,?,?)""",
                            (userId, first_name, last_name, address, experience_level, is_dedicator, datetime.now(), '2040-01-01', existing_user[9]+1, 1))

                print(f"Updated user {userId} in dimUser table")
            else:
                print(f"No changes for user {userId}")
    cursor_dwh.commit()


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

        # # only runs for the first time
        # insert_first_run_data(cursor_op,cursor_dwh)

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
#
# import datetime
# import pyodbc
#
# from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
# from dwh import establish_connection
#
#
# # Define query to extract user attribute with a new 'found_logs_no' column
# user_query = """
# SELECT
#     u.id,
#     u.first_name,
#     u.last_name,
#     u.number,
#     u.street,
#     c.city_name,
#     co.name as country_name,
#     COUNT(tl.id) OVER (PARTITION BY u.id) as found_logs_no,
#     MAX(CASE WHEN t.owner_id IS NOT NULL THEN 'Yes' ELSE 'No' END) AS is_dedicator,
#     MIN(tl.log_time) as earliest_log_date
# FROM    catchem_9_2023.dbo.user_table u
# LEFT JOIN    catchem_9_2023.dbo.city c ON u.city_city_id = c.city_id
# LEFT JOIN    catchem_9_2023.dbo.country co ON c.country_code = co.code
# LEFT JOIN    catchem_9_2023.dbo.treasure_log tl ON u.id = tl.hunter_id
# LEFT JOIN    catchem_9_2023.dbo.treasure t ON u.id = t.owner_id
# """
#
# # Function to create dimUser table if it doesn't exist
# def create_dimUser_table(conn):
#     cursor = conn.cursor()
#
#     # SQL query to create dimUser table
#     create_table_query = """
#     IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dimUser')
#     BEGIN
#         CREATE TABLE dimUser (
#             user_SK INT IDENTITY(1,1) NOT NULL,
#             userId BINARY(16) NOT NULL,
#             first_name NVARCHAR(255) NOT NULL,
#             last_name NVARCHAR(255) NOT NULL,
#             number NVARCHAR(255) NOT NULL,
#             street NVARCHAR(255) NOT NULL,
#             city_name NVARCHAR(255) NOT NULL,
#             country_name NVARCHAR(255) NOT NULL,
#             experience_level NVARCHAR(50) NOT NULL,
#             is_dedicator NVARCHAR(3) NOT NULL,
#             scd_start DATETIME NOT NULL,
#             scd_end DATETIME,
#             scd_version INT,
#             scd_active BIT
#         );
#     END
#     """
#
#     try:
#         cursor.execute(create_table_query)
#         conn.commit()
#         print("dimUser table created successfully or already exists")
#     except pyodbc.Error as e:
#         print(f"Error creating dimUser table: {e}")
#     finally:
#         cursor.close()
#
#

# def handle_dimUser_scd(cursor_op, cursor_dwh, first_run=False):
#     if first_run:
#         insert_first_run_data(cursor_op, cursor_dwh)
#         return
#
#     # Execute the user_query
#     print("Extracting user data from cachem db...")
#     cursor_op.execute(user_query)
#     print("Query execution complete")
#     rows = cursor_op.fetchall()
#
#     for row in rows:
#         userId, first_name, last_name, number, street, city_name, country_name, found_logs_no, is_dedicator, earliest_log_date = row
#
#         # Determine experience_level
#         if found_logs_no == 0:
#             experience_level = 'Starter'
#         elif found_logs_no < 4:
#             experience_level = 'Amateur'
#         elif 4 <= found_logs_no <= 10:
#             experience_level = 'Professional'
#         else:
#             experience_level = 'Pirate'
#
#         # Determine SCD date
#         scd_start = earliest_log_date if earliest_log_date else datetime.datetime.now()
#         scd_end = '2040-01-01'  # set far in the future
#
#         # Look up existing data
#         select_latestSCD = """
#             SELECT MAX(scd_version), MAX(scd_end)
#             FROM dimUser
#             WHERE userId = ?
#         """
#
#         select_addressAndDedicator = """
#             SELECT number, street, city_name, country_name, is_dedicator
#             FROM dimUser
#             WHERE userId = ?
#         """
#
#         cursor_dwh.execute(select_latestSCD, (userId,))
#         scd_result = cursor_dwh.fetchone()
#
#         cursor_dwh.execute(select_addressAndDedicator, (userId,))
#         addressAndDedicator_result = cursor_dwh.fetchone()
#
#         if scd_result is not None and addressAndDedicator_result is not None:
#             combined_result = scd_result + addressAndDedicator_result
#             scd_version, max_scd_end, existing_number, existing_street, existing_city_name, existing_country_name, existing_is_dedicator = combined_result
#         else:
#             print("No data found for the provided userId")
#             continue
#
#         if max_scd_end and max_scd_end > scd_start:
#             scd_start = max_scd_end
#
#         if scd_version:
#             scd_version += 1
#         else:
#             scd_version = 1
#
#         if (existing_number != number or existing_street != street or existing_city_name != city_name
#                 or existing_country_name != country_name or existing_is_dedicator != is_dedicator):
#             scd_version += 1
#
#         # Insert or update record in the data warehouse
#         insert_query = """
#             INSERT INTO dimUser (userId, firstName, lastName, number, street, city_name, country_name,
#              experience_level, scd_start, scd_end, scd_version, scd_active)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
#         """
#         cursor_dwh.execute(insert_query, (userId, first_name, last_name, number, street, city_name, country_name,
#                                           experience_level, scd_start, scd_end, scd_version, 1))
#
#         if max_scd_end:
#             update_query = """
#                 UPDATE dimUser
#                 SET scd_end = ?, scd_active = ?
#                 WHERE userId = ? AND scd_version = ? AND scd_active = 1
#             """
#             cursor_dwh.execute(update_query, (scd_start, 0, userId, scd_version - 1))
#
#         cursor_dwh.commit()
#         print("SCD Type 2 handling for dimUser completed successfully")
#
#
# def main():
#     try:
#         # Connect to the 'catchem' database
#         conn_op = establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
#         cursor_op = conn_op.cursor()
#
#         # Connect to the 'catchem_dwh' database
#         conn_dwh = establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
#         cursor_dwh = conn_dwh.cursor()
#
#         # Create dimUser table if not exists
#         create_dimUser_table(conn_dwh)
#
#         # Handle SCD Type 2 updates for dimUser on first run
#         handle_dimUser_scd(cursor_op, cursor_dwh, first_run=True)
#
#     except pyodbc.Error as e:
#         print(f"Error connecting to the database: {e}")
#
#     finally:
#         # Close connections
#         cursor_op.close()
#         cursor_dwh.close()
#         conn_op.close()
#         conn_dwh.close()
#
#
# if __name__ == "__main__":
#     main()
