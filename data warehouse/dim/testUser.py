import datetime
from datetime import datetime
import pyodbc
import pandas as pd

from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER, DSN
from dwh import establish_connection

# Define query to extract user attribute

def insert(cursor_op, cursor_dwh):
    # Define SQL query to fetch data from the source table
    select_query = """
        SELECT u.id, u.first_name, u.last_name, u.number, u.street, c.city_name, co.name as country_name,
               COUNT(tl.id) as found_logs_count, 
               MAX(CASE WHEN tre.owner_id IS NOT NULL THEN 1 ELSE 0 END) as is_dedicator,
               MIN(tl.log_time) as earliest_log_date
        FROM user_table u
        LEFT JOIN city c ON u.city_city_id = c.city_id
        LEFT JOIN country co ON c.country_code = co.code
        LEFT JOIN treasure_log tl ON u.id = tl.hunter_id
        LEFT JOIN treasure tre ON u.id = tre.owner_id
        GROUP BY u.id, u.first_name, u.last_name, u.number, u.street, c.city_name, co.name
    """
    cursor_op.execute(select_query)

    for row in cursor_op.fetchall():
        user_id, first_name, last_name, number, street, city, country, found_logs_count, is_dedicator, earliest_log_date = row

        # Determine experience level
        if found_logs_count == 0:
            experience_level = 'Starter'
        elif found_logs_count < 4:
            experience_level = 'Amateur'
        elif 4 <= found_logs_count <= 10:
            experience_level = 'Professional'
        else:
            experience_level = 'Pirate'

        # Determine SCD dates
        scd_start = earliest_log_date if earliest_log_date else datetime.datetime.now()
        scd_end = '2040-01-01'  # End date far in the future

        # Check if user exists in the data warehouse
        select_user_query = """
            SELECT MAX(scd_version), MAX(scd_end)
            FROM dimUser
            WHERE userId = ?
        """
        cursor_dwh.execute(select_user_query, (user_id,))
        scd_version, max_scd_end = cursor_dwh.fetchone()

        if max_scd_end and max_scd_end > scd_start:
            scd_start = max_scd_end  # Start from the last end date

        if scd_version:
            scd_version += 1
        else:
            scd_version = 1

        # Insert or update records in the data warehouse
        insert_query = """
            INSERT INTO dimUser (userId, firstName, lastName, number, street, city, country, experienceLevel, isDedicator, scd_start, scd_end, scd_version, scd_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor_dwh.execute(insert_query, (user_id, first_name, last_name, number, street, city, country, experience_level, is_dedicator, scd_start, scd_end, scd_version, 1))

        if max_scd_end:
            update_query = """
                UPDATE dimUser
                SET scd_end = ?, scd_active = ?
                WHERE userId = ? AND scd_version = ? AND scd_active = 1
            """
            cursor_dwh.execute(update_query, (scd_start, 0, user_id, scd_version - 1))

    cursor_dwh.commit()

