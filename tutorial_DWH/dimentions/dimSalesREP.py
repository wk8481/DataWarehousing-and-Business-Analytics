import pyodbc
import datetime
import config

import dwh_tools as dwh
# Define the connection parameters
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD,DRIVER)
conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD,DRIVER)


# Create cursors
cursor_op = conn_op.cursor()
cursor_dwh = conn_dwh.cursor()

# Define SQL query to fetch data from the source table
select_query = """SELECT salesRepID, name, office FROM salesrep"""
cursor_op.execute(select_query)

# Loop through the rows fetched from the source table
for sales_rep_id, name, office_op in cursor_op.fetchall():
    # Execute query to get office and scd_version from dimSalesREP
    cursor_dwh.execute("SELECT office, scd_version FROM dimSalesREP WHERE salesRepID = ? AND scd_active = 1",
                       sales_rep_id)
    rows = cursor_dwh.fetchall()

    if not rows:
        # Insert a new record into dimSalesREP if no record exists for the salesRepID
        insert_query = """INSERT INTO dimSalesREP (salesRepId, name, office, scd_start, scd_end, scd_version, scd_active)
                          VALUES (?, ?, ?, ?, ?, ?, ?)"""
        cursor_dwh.execute(insert_query, (sales_rep_id, name, office_op, datetime.datetime.now(), '2040-01-01', 1, 1))
    else:
        office_latest, scd_version_latest = max(rows, key=lambda x: x[1])

        if office_op != office_latest:
            # Update the SCD attributes of the latest version
            update_query = """UPDATE dimSalesREP SET scd_end = ?, scd_version = ?, scd_active = ?
                              WHERE salesRepId = ? AND scd_active = 1"""
            cursor_dwh.execute(update_query, (datetime.datetime.now(), scd_version_latest, 0, sales_rep_id))

            # Insert a new record into dimSalesREP with a new version
            insert_query = """INSERT INTO dimSalesREP (salesRepId, name, office, scd_start, scd_end, scd_version, scd_active)
                              VALUES (?, ?, ?, ?, ?, ?, ?)"""
            cursor_dwh.execute(insert_query, (
            sales_rep_id, name, office_op, datetime.datetime.now(), '2040-01-01', scd_version_latest + 1, 1))

    # Commit the transaction for each row
    conn_dwh.commit()

# Close the cursors and connections
cursor_op.close()
cursor_dwh.close()
conn_op.close()
conn_dwh.close()
#
# cursor_dwh.execute(select_latestSCD, (userId,))
# scd_result = cursor_dwh.fetchone()
#
# cursor_dwh.execute(select_addressAndDedicator, (userId,))
# addressAndDedicator_result = cursor_dwh.fetchone()
#
# if scd_result is not None and addressAndDedicator_result is not None:
#     scd_version, latest_scd_end = scd_result
#     existing_number, existing_street, existing_city_name, existing_country_name, existing_is_dedicator = addressAndDedicator_result
# else:
#     print("No data found for the provided userId")
#     continue
#
# if latest_scd_end and latest_scd_end > scd_start:  # if max_scd_end is not null, it's later than sdc_start
#     scd_start = latest_scd_end  # set start from last end date
#
# if scd_version:
#     scd_version += 1
# else:
#     scd_version = 1
# #
# # # Check if any of the attributes have changed
# # if (existing_number != number or existing_street != street or existing_city_name != city_name
# #         or existing_country_name != country_name or existing_is_dedicator != is_dedicator):
# #     scd_version += 1
# #
# # # Insert or update record in the data warehouse
# insert_query = """
#       INSERT INTO dimUser (userId, first_name, last_name, number, street, city_name, country_name,
#        experience_level, scd_start, scd_end, scd_version, scd_active)
#       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
#   """
# cursor_dwh.execute(insert_query, (userId, first_name, last_name, number, street, city_name, country_name,
#                                   experience_level, scd_start, scd_end, scd_version, 1))
#
# if latest_scd_end:
#     update_query = """
#           UPDATE dimUser
#           SET scd_end = ?, scd_active = ?
#           WHERE userId = ? AND scd_version = ? AND scd_active = 1
#       """
#     cursor_dwh.execute(update_query, (scd_start, 0, userId, scd_version - 1))
