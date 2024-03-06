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


