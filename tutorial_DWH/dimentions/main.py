import subprocess
import pyodbc
# Run dimDay.py
subprocess.run(["python", "dimDay.py"])

# Run dimSalesREP.py
subprocess.run(["python", "dimSalesREP.py"])

# Run FactSales.py
subprocess.run(["python", "FactSales.py"])
