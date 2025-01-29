import pyodbc
import pandas as pd
import re

# Establish connection to SQL Server
sql_server_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=127.0.0.1;"  # Replace with your server address
    "DATABASE=master;"  # Replace with your database name
    "UID=sa;"  # Replace with your SQL Server username
    "PWD=Password123!;"  # Replace with your SQL Server password
    "TrustServerCertificate=yes;"
)

# Query to fetch employee data from the database
with sql_server_conn.cursor() as sql_cursor:
  # Export as CSV
    sql_cursor.execute(
        """
        SELECT [EmployeeID]
              ,[LastName]
              ,[FirstName]
              ,[Title]
              ,[TitleOfCourtesy]
              ,[BirthDate]
              ,[HireDate]
              ,[Address]
              ,[City]
              ,[Region]
              ,[PostalCode]
              ,[Country]
              ,[HomePhone]
              ,[Extension]
              ,[ReportsTo]
        FROM [master].[dbo].[Employees]
        """
    )

    # Fetch all results
    employee_data_raw = sql_cursor.fetchall()

# Ensure the data is unpacked properly into a list of rows
employee_data = [list(row) for row in employee_data_raw]

# Define column names based on the SELECT statement
columns = [
    "EmployeeID",
    "LastName",
    "FirstName",
    "Title",
    "TitleOfCourtesy",
    "BirthDate",
    "HireDate",
    "Address",
    "City",
    "Region",
    "PostalCode",
    "Country",
    "HomePhone",
    "Extension",
    "ReportsTo",
]

# Convert to a pandas DataFrame
employee_data_df = pd.DataFrame(employee_data, columns=columns)

date_columns = ['BirthDate', 'HireDate']
for col in date_columns:
    employee_data_df[col] = pd.to_datetime(employee_data_df[col]).dt.strftime('%Y-%m-%d %hh:%mm:%ss')

employee_data_df = employee_data_df.fillna("null")

# Convert all columns to strings
employee_data_df = employee_data_df.astype(str)

employee_data_df["ReportsTo"] = employee_data_df["ReportsTo"].apply(
    lambda x: re.sub(r'\.0$', '', x)  # Remove only ".0" at the end of the string
)

# Read the expected data from the CSV file
expected_employees = pd.read_csv("expected_employees.csv", dtype=str)
expected_employees = expected_employees.fillna("null")

date_columns = ['BirthDate', 'HireDate']
for col in date_columns:
    expected_employees[col] = pd.to_datetime(expected_employees[col]).dt.strftime('%Y-%m-%d %hh:%mm:%ss')

print(employee_data_df)
print(expected_employees)

# Compare the two DataFrames
# Example: Check for mismatched rows
mismatched_rows = pd.concat([employee_data_df, expected_employees]).drop_duplicates(keep=False)

# Save mismatched rows to a CSV file (if needed)
mismatched_rows.to_csv("mismatched_employees.csv", index=False)

# Output mismatched rows (if any)
if mismatched_rows.empty:
    print("All data matches between the SQL Server and the expected employees CSV.")
else:
    print("Mismatched rows found! Check 'mismatched_employees.csv' for details.")
