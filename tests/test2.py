import pyodbc
import json
import pandas as pd
from datetime import datetime


def fetch_and_serialize(connection_string, query):
    # Connect to the database
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Execute the query
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    dtypes = [str(cursor.description[i][1]) for i in range(len(columns))]

    # Prepare the data structure
    data = {
        "schema": {
            "columns": [{"name": col, "dtype": dtype} for col, dtype in zip(columns, dtypes)]
        },
        "data": []
    }

    # Convert rows to a list of lists, handling datetime objects
    for row in rows:
        serialized_row = []
        for value in row:
            if isinstance(value, datetime):
                serialized_row.append(value.isoformat())  # Convert datetime to ISO format string
            else:
                serialized_row.append(value)
        data["data"].append(serialized_row)

    # Serialize to JSON
    json_data = json.dumps(data, indent=4)

    # Close the connection
    cursor.close()
    conn.close()

    return json_data


def dataframe_from_json(json_data):
    data = json.loads(json_data)

    # Extract schema and data
    columns = [col['name'] for col in data['schema']['columns']]
    dtypes = [col['dtype'] for col in data['schema']['columns']]
    rows = data['data']

    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Map SQL data types to pandas data types
    dtype_mapping1 = {
        'int': 'object',
        'float': 'object',
        'str': 'object',
        'bool': 'bool',
        'datetime.datetime': 'datetime64[ns]',  # Map datetime to pandas datetime
        # Add more mappings as needed
    }

    dtype_mapping = {}
    for k,v in dtype_mapping1.items():
        dtype_mapping[f"<class '{k}'>"] = v

    # Apply dtypes to DataFrame columns
    for col, dtype in zip(columns, dtypes):
        pandas_dtype = dtype_mapping.get(dtype, 'object')  # Default to 'object' if dtype is not mapped
        df[col] = df[col].astype(pandas_dtype)

    return df


def dataframe_from_db(connection_string, query):
    conn = pyodbc.connect(connection_string)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def compare_dataframes(df1, df2):
    # Check if the DataFrames are equal, including dtypes
    mismatched_rows = pd.concat([df1, df2]).drop_duplicates(keep=False)
    mismatched_rows.to_csv("mismatched_employees.csv", index=False)
    return len(mismatched_rows) <= 1


# Example usage
connection_string = ("DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1;"
                     "DATABASE=master;UID=sa;PWD=Password123!;TrustServerCertificate=yes;")


query = '''SELECT [EmployeeID]
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
        WHERE [EmployeeID] > 9
        '''

# Read serialized data from JSON
with open('serialized_data.json', 'r') as serialized_json:
    serialized_data = serialized_json.read()

# Construct DataFrame from serialized JSON
df_from_json = dataframe_from_json(serialized_data)

# Construct DataFrame directly from database
df_from_db = dataframe_from_db(connection_string, query)

# Compare DataFrames
are_equal = compare_dataframes(df_from_json, df_from_db)
if not are_equal:
    raise Exception('Mismatch found. mismatched_employees.csv for reference')
print("Test Passed!")