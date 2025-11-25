import pyodbc

print("Your Python environment can see the following ODBC drivers:")
for driver in pyodbc.drivers():
    print(f"- {driver}")

    