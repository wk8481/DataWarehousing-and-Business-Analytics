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
    u.number,
    u.street,
    c.city_name,
    co.name as country_name,
    COUNT(tl.id) as found_logs_no,
    MAX(CASE WHEN t.owner_id IS NOT NULL THEN 'Yes' ELSE 'No' END) AS is_dedicator,
    MIN(tl.log_time) as earliest_log_date
FROM    user_table u
LEFT JOIN    city c ON u.city_city_id = c.city_id
LEFT JOIN    country co ON c.country_code = co.code
LEFT JOIN    treasure_log tl ON u.id = tl.hunter_id
LEFT JOIN    treasure t ON u.id = t.owner_id
GROUP BY u.id, u.first_name, u.last_name, u.number, u.street, c.city_name, co.name
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
            userId BINARY(16) NOT NULL,    
            first_name NVARCHAR(255) NOT NULL,
            last_name NVARCHAR(255) NOT NULL,
            number NVARCHAR(255) NOT NULL,
            street NVARCHAR(255) NOT NULL,
            city_name NVARCHAR(255) NOT NULL,
            country_name NVARCHAR(255) NOT NULL,
            experience_level NVARCHAR(50) NOT NULL,
            is_dedicator NVARCHAR(3) NOT NULL,
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




# Version 2

# def create_missing_records(cursor_dwh, dim_rows, user_id, first_name, last_name, city_name, country_name, dedicator,
#                            experience_level):
#     try:
#         # Check if there are missing records for the provided experience level
#         if experience_level == 'Pirate' and not any(row[5] == 'Pirate' for row in dim_rows):
#             # Insert missing historical record for 'Pirate'
#             insert_historical_record(cursor_dwh, user_id, first_name, last_name, city_name, country_name,
#                                      'Pirate', dedicator)
#         elif experience_level == 'Professional' and not any(row[5] == 'Professional' for row in dim_rows):
#             # Insert missing historical record for 'Professional'
#             insert_historical_record(cursor_dwh, user_id, first_name, last_name, city_name, country_name,
#                                      'Professional', dedicator)
#         elif experience_level == 'Amateur' and not any(row[5] == 'Amateur' for row in dim_rows):
#             # Insert missing historical record for 'Amateur'
#             insert_historical_record(cursor_dwh, user_id, first_name, last_name, city_name, country_name,
#                                      'Amateur', dedicator)
#         elif experience_level == 'Starter' and not any(row[5] == 'Starter' for row in dim_rows):
#             # Insert missing historical record for 'Starter'
#             insert_historical_record(cursor_dwh, user_id, first_name, last_name, city_name, country_name,
#                                      'Starter', dedicator)
#     except pyodbc.Error as e:
#         print(f"Error creating missing records: {e}")
#
# def insert_historical_record(cursor_dwh, user_id, first_name, last_name, city_name, country_name, experience_level,
#                              dedicator):
#     try:
#         # Insert a new historical record for the user with the provided details and experience level
#         insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
#                                                 experience_level, dedicator, scd_start, scd_end, scd_version,
#                                                 scd_active)
#                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
#         cursor_dwh.execute(insert_query, (user_id, first_name, last_name, city_name, country_name,
#                                           experience_level, dedicator, datetime.now(),
#                                           '', latest_version + 1, 1))
#     except pyodbc.Error as e:
#         print(f"Error inserting historical record: {e}")


# def get_earliest_found_log_time(conn_op, user_id):
#     try:
#         conn_op.execute("""
#         SELECT MIN(log_time)
#         FROM treasure_log
#         WHERE hunter_id = ? AND log_type = 2
#         """, user_id )
#         earliest_log_time = conn_op.fetchone()[0]
#         return earliest_log_time if earliest_log_time else None
#     except pyodbc.Error as e :
#         print(f"Error retrieveing earlieset found log time for user{user_id}: {e}")
#         return None
#
#
# def get_earliest_log_times(cursor_op, cursor_dwh, user_id, first_name, last_name, city_name, country_name, dedicator,
#                            experience_level, latest_version):
#     try:
#         # Get the earliest log time for each experience level
#         log_time_query = """
#         SELECT hunter_id, log_time, ROW_NUMBER() OVER(PARTITION BY hunter_id ORDER BY log_time) AS row_num
#         FROM treasure_log
#         WHERE hunter_id = ? AND log_type = 2
#         """
#         cursor_op.execute(log_time_query, user_id)
#         log_rows = cursor_op.fetchall()
#
#         for log_row in log_rows:
#             hunter_id, log_time, row_num = log_row
#             if row_num == 1:
#                 starter_log_time = log_time
#                 starter_insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
#                                                               experience_level, dedicator, scd_start, scd_end, scd_version,
#                                                               scd_active)
#                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
#                 cursor_dwh.execute(starter_insert_query, (user_id, first_name, last_name, city_name, country_name,
#                                                           'Starter', dedicator, starter_log_time,
#                                                           datetime.now(), latest_version + 1, 1))
#             elif row_num == 4:
#                 amateur_log_time = log_time
#                 amateur_insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
#                                                               experience_level, dedicator, scd_start, scd_end, scd_version,
#                                                               scd_active)
#                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
#                 cursor_dwh.execute(amateur_insert_query, (user_id, first_name, last_name, city_name, country_name,
#                                                           'Amateur', dedicator, amateur_log_time,
#                                                           datetime.now(), latest_version + 1, 1))
#             elif row_num == 11:
#                 professional_log_time = log_time
#                 professional_insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
#                                                                    experience_level, dedicator, scd_start, scd_end, scd_version,
#                                                                    scd_active)
#                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
#                 cursor_dwh.execute(professional_insert_query, (user_id, first_name, last_name, city_name, country_name,
#                                                                'Professional', dedicator, professional_log_time,
#                                                                datetime.now(), latest_version + 1, 1))
#             elif row_num > 10:
#                 pirate_log_time = log_time
#                 pirate_insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
#                                                            experience_level, dedicator, scd_start, scd_end, scd_version,
#                                                            scd_active)
#                                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
#                 cursor_dwh.execute(pirate_insert_query, (user_id, first_name, last_name, city_name, country_name,
#                                                          'Pirate', dedicator, pirate_log_time,
#                                                          datetime.now(), latest_version + 1, 1))
#     except pyodbc.Error as e:
#         print(f"Error getting earliest log times: {e}")


# Function to handle SCD Type 2 updates for dimUser table
def handle_dimUser_scd(cursor_op, cursor_dwh):
    # these code added to check if 'cursor' is a cursor object
    # since we got AttributeError:: 'pyodbc.Cursor' object has no attribute 'cursor'

        # Execute the user_query
        print("Extracting user data from cachem db...")
        cursor_op.execute(user_query)
        print("Query execution complete")
        rows = cursor_op.fetchall()

    
        for row in rows:
            # Extract data from the source table
            userId, first_name, last_name, number, street, city_name, country_name, found_logs_no, is_dedicator, earliest_log_date = row

            # Determine experience_level
            if found_logs_no == 0:
                experience_level = 'Starter'
            elif found_logs_no < 4:
                experience_level = 'Amateur'
            elif 4 <= found_logs_no <= 10:
                experience_level = 'Professional'
            else:
                experience_level = 'Pirate'

            # Determine SCD date
            scd_start = earliest_log_date if earliest_log_date else datetime.now()
            scd_end = '2040-01-01' # set far in the future

            # use partition by for query
            # modify this query this causing error
            # Check if user exists in the data warehouse
            select_user_query = """
            SELECT MAX(scd_version), MAX(scd_end), number, street, city_name, country_name, is_dedicator
            FROM dimUser
            WHERE userId = ?
            """

            cursor_dwh.execute(select_user_query, (userId))
            scd_version, max_scd_end, existing_number, existing_street, existing_city_name, existing_country_name, existing_is_dedicator = cursor_dwh.fetchone()

            if max_scd_end and max_scd_end > scd_start: # if max_scd_end is not null, it's later than sdc_start
                scd_start = max_scd_end # set start from last end date
            if scd_version:
                scd_version += 1
            else:
                scd_version = 1

            # Check if any of the attributes have changed
            if (existing_number != number or existing_street != street or existing_city_name != city_name
            or existing_country_name != country_name or existing_is_dedicator != is_dedicator):
                scd_version += 1

            # Insert or update record in the data warehouse
            insert_query = """
            INSERT INTO dimUser (userId, firstName, lastName, number, street, city_name, country_name,
             experience_level, scd_start, scd_end, scd_version, scd_active)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """
            cursor_dwh.execute(insert_query, (userId, first_name, last_name, number, street, city_name, country_name,
                                experience_level, scd_start, scd_end, scd_version,1))

            if max_scd_end:
                update_query = """
            UPDATE dimUser
            SET scd_end = ?, scd_active = ?
            WHERE userId = ? AND scd_version = ? AND scd_active = 1
            """

            cursor_dwh.execute(update_query, (scd_start, 0, userId, scd_version - 1))


            cursor_dwh.commit()
            print("SCD Type 2 handling for dimUser completed successfully")

        #
        #     # Execute query to check SCD Type 2 for userId
        #     cursor_dwh.execute("SELECT userId, scd_version FROM dimUser WHERE userId = ? AND scd_active = 1",
        #                        user_id)
        #     dim_rows = cursor_dwh.fetchall()
        #
        #     if not dim_rows:
        #         # Insert a new record into dimUser if no record exists for the userId
        #         insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
        #                                                 experience_level, dedicator, scd_start, scd_end, scd_version,
        #                                                 scd_active)
        #                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        #         cursor_dwh.execute(insert_query, (user_id, first_name, last_name, city_name, country_name,
        #                                           experience_level, dedicator, datetime.now(),
        #                                           '', 1, 1))
        #
        #
        #     else:
        #         # Get the latest version and attributes
        #         latest_version = max(dim_rows, key=lambda x: x[1])[1]
        #
        #         # checks if there are any differences between the attributes of the current row from op database & the rows in the dwh
        #         if any(row != dim_row[:-2] for dim_row in dim_rows) or has_become_dedicator(user_id):
        #             # Update the SCD attributes of the latest version if any changes
        #             update_query = """UPDATE dimUser SET scd_end = ?, scd_version = ?, scd_active = ?
        #                               WHERE userId = ? AND scd_active = 1"""
        #             cursor_dwh.execute(update_query, (datetime.now(), latest_version, 0, user_id))
        #
        #             # Insert a new record into dimUser with a new version
        #             insert_query = """INSERT INTO dimUser (userId, first_name, last_name, city_name, country_name,
        #                                                   experience_level, dedicator, scd_start, scd_end, scd_version,
        #                                                   scd_active)
        #                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        #             cursor_dwh.execute(insert_query, (user_id, first_name, last_name, city_name, country_name,
        #                                               experience_level, dedicator, datetime.now(),
        #                                               '', latest_version + 1, 1))
        #
        #             # Check for missing historical records based on log times
        #             get_earliest_log_times(cursor_op, cursor_dwh, user_id, first_name, last_name, city_name,
        #                                    country_name, dedicator,
        #                                    experience_level, latest_version)
        #
        #             # Create missing historical records if needed
        #             create_missing_records(cursor_dwh, dim_rows, user_id, first_name, last_name, city_name,
        #                                    country_name,
        #                                    dedicator, experience_level)
        #
        # # compare user address in operational database and if there is changes
        # # Commit the transaction for all rows


    # except pyodbc.Error as e:
    #     print(f"Error handling SCD Type 2 for dimUser: {e}")
    # finally:
    #     if not isinstance(conn_op, pyodbc.Cursor):
    #         cursor_op.close()
    #     if not isinstance(conn_dwh, pyodbc.Cursor):
    #         cursor_dwh.close()


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
