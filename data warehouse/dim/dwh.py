import pyodbc
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER


def establish_connection(server=SERVER, database=DATABASE_OP, username=USERNAME, password=PASSWORD,driver=DRIVER):
    """
    Establishes a connection to the specified SQL Server database.
    Args:
        server (str): The server name or IP address.
        database (str): The name of the database.
        username (str): The username for authentication.
        password (str): The password for authentication.
        driver (str): default '{SQL Server}'
    Returns:
        pyodbc.Connection: The connection object.
    """
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    return pyodbc.connect(connection_string)