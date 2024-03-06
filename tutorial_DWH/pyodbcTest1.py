
import pyodbc

connection_url_db = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=catchem;UID=SA;PWD=DB3&3DB&DB3;TrustServerCertificate=yes"

try:
    cnxn = pyodbc.connect(connection_url_db)

    cursor = cnxn.cursor()
    print(" Successfully connected to the DataBase-> ", )


except pyodbc.Error as err:
    print(f"Error: {err}")

finally:
    if cursor:
        cursor.close()
