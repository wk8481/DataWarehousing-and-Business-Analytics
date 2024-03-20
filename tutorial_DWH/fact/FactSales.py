import pyodbc
import dwh_tools as dwh

try:
    # Create connection strings
    from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
    conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
    conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

    # Create cursors
    cursor_op = conn_op.cursor()
    cursor_dwh = conn_dwh.cursor()

    # Fetch order data from sales
    Sales_query = "SELECT Order_Date, Customer_Name, SalesRepId, Amount, Order_ID FROM sales"
    cursor_op.execute(Sales_query)
    for row in cursor_op.fetchall():
        Order_Date, Customer_Name, SalesRepId, Amount, Order_ID = row
        # Check if the sales record already exists in the fact table
        FactSales_query = "SELECT [SALES_ID] FROM FactSales WHERE [SALES_ID] = ?"
        cursor_dwh.execute(FactSales_query, Order_ID)

        if not cursor_dwh.fetchone():  # If the record doesn't exist, we want to only insert SK
            # Fetch DIM_DATE_SK
            dimDay_query = "SELECT Date_SK FROM dimDay WHERE DATE = ?"
            cursor_dwh.execute(dimDay_query, Order_Date)
            Date_SK = cursor_dwh.fetchone()
            if Date_SK:
                Date_SK = Date_SK[0]
            else:
                # Handle the case when Date_SK is not found
                print(f"Error: Date_SK not found for date: {Order_Date}")
                continue

            # Fetch DIM_SALESREP_SK
            SalesRep_query = "SELECT salesRepSK FROM dimSalesRep WHERE salesRepID = ? AND scd_active = 1"
            cursor_dwh.execute(SalesRep_query, SalesRepId)
            salesRepSK = cursor_dwh.fetchone()
            if salesRepSK:
                salesRepSK = salesRepSK[0]
            else:
                # Handle the case when salesRepSK is not found
                print(f"Error: salesRepSK not found for salesRepID: {SalesRepId}")
                continue

            # Insert into FactSales
            insert_query = """INSERT INTO [dbo].[FactSales] ([SALES_ID], [DIM_DATE_SK], [DIM_SALESREP_SK], [REVENUE_MV], [COUNT_MV])
                                  VALUES (?, ?, ?, ?, ?)"""
            cursor_dwh.execute(insert_query, (Order_ID, Date_SK, salesRepSK, Amount, 1))
        conn_dwh.commit()
    #close cursors
    cursor_op.close()
    cursor_dwh.close()
    #close connections
    conn_op.close()
    conn_dwh.close()
except pyodbc.Error as e:
    print(f"Error connecting to the database: {e}")

