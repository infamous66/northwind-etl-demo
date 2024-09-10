import pymysql
import pyodbc

# MySQL connection (Chinook database)
mysql_conn = pymysql.connect(
    host="localhost", user="root", password="123852", database="chinook_autoincrement",
)


# SQL server connection (Northwind database)
sql_server_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=WIN-Q9N7UMNNT2O;"
    "DATABASE=Northwind;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


# Retrieve data from Chinook Employee table
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT EmployeeId, LastName, FirstName, Title, ReportsTo,
        BirthDate, HireDate, Address, City, State, Country, PostalCode, Phone
        FROM Employee;
        """
    )
    employees_data = cursor.fetchall()
print("Employee data retrieved.")

# Add Temporary columns to Northwind Employees table to store old ids
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Employees
            ADD chinook_emp_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Employees
            ADD chinook_reports_to_id INT NULL;
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction failed while adding columns to Employee table: {e}")


# Insert data into Northwind Employees table
insert_query = """
INSERT INTO Employees (
chinook_emp_id, LastName, FirstName, Title, chinook_reports_to_id, BirthDate, HireDate, Address,
City, Region, Country, PostalCode, HomePhone)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, employees_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction failed while inserting employee data: {e}")


# Update ReportsTo column
update_query = """
UPDATE e
SET e.reportsTo = nw_employee.EmployeeID
FROM Employees e
INNER JOIN Employees nw_employee
    ON nw_employee.chinook_emp_id = e.chinook_reports_to_id
WHERE e.chinook_reports_to_id IS NOT NULL;
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(update_query)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while updating ReportsTo column: {e}")
print("Employee data inserted successfully")


# Retrieve data from Chinook Customer table,
# transform FirstName and LastName columns to match Northwind ContactName column
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT CustomerId, Company, CONCAT(FirstName, ' ', Lastname) AS ContactName, 
        Address, City, State, PostalCode, Country, Phone, Fax
        FROM Customer
        """
    )
    customer_data = cursor.fetchall()
print("Customer data retrieved")

# Transform columns in Northwind Customers table to avoid data truncation
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Customers
            ALTER COLUMN CompanyName NVARCHAR(80);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Customers
            ALTER COLUMN ContactName NVARCHAR(60);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Customers
            ALTER COLUMN Address NVARCHAR(70);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Customers
            ALTER COLUMN City NVARCHAR(50);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Customers
            ALTER COLUMN Region NVARCHAR(40);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Customers
            ALTER COLUMN Country NVARCHAR(40);
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction failed while transforming Customers columns: {e}")


# Insert customer data into Northwind Customers table
insert_query = """
INSERT INTO CUSTOMERS (
CustomerID, CompanyName, ContactName, Address, City, Region, PostalCode, Country, Phone, Fax)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, customer_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction failed while inserting customer data: {e}")
print("Customer data inserted successfully")

# Retrieve data from Chinook Invoice table
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT i.customerId, SupportRepId, InvoiceDate, BillingAddress, 
        BillingCity, BillingState, BillingPostalCode, BillingCountry, InvoiceId
        FROM Invoice i INNER JOIN Customer c ON c.CustomerId = i.customerId
        """
    )
    order_data = cursor.fetchall()
print("Invoice data retrieved")

# Create a temporary columns to Northwind Orders table,
# Transform columns to avoid data truncation
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Orders
            ADD chinook_invoice_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Orders
            ADD chinook_support_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Orders
            ALTER COLUMN ShipCity NVARCHAR(40);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Orders
            ALTER COLUMN ShipRegion NVARCHAR(40);
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Orders
            ALTER COLUMN ShipCountry NVARCHAR(40);
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction failed while adding columns to Orders table: {e}")


# Insert order data into Northwind Orders table
insert_query = """
INSERT INTO Orders (
CustomerID, chinook_support_id, OrderDate, ShipAddress, ShipCity, 
ShipRegion, ShipPostalCode, ShipCountry, chinook_invoice_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, order_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while inserting order data: {e}")


# Update EmployeeID column in Orders table
update_query = """
UPDATE o
SET o.EmployeeID = e.EmployeeID
FROM Orders o
INNER JOIN Employees e
    ON o.chinook_support_id = e.chinook_emp_id
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(update_query)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while updating EmployeeID in Orders table: {e}")
print("Order data inserted successfully")

# Retrieve data from Chinook Genre table
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT GenreId, Name
        FROM Genre
        """
    )
    genre_data = cursor.fetchall()
print("Genre data retrieved")

# Add temporary column to Northwind Categories table
# Transform columns to avoid data truncation
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Categories
            ADD chinook_genre_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Categories
            ALTER COLUMN CategoryName NVARCHAR(120);
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while adding columns to Categories table: {e}")


# Insert Genre data to Categories table
insert_query = """
INSERT INTO Categories (
chinook_genre_id, CategoryName)
VALUES (?, ?)
"""
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, genre_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while inserting genre data: {e}")
print("Genre data inserted successfully")

# Retrieve data from Chinook Artist table
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT ArtistId, Name
        FROM Artist
        """
    )
    artist_data = cursor.fetchall()
print("Artist data retrieved")

# Add temporary column to Northwind Suppliers table,
# Transform column to avoid data truncation
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Suppliers
            ADD chinook_artist_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Suppliers
            ALTER COLUMN CompanyName NVARCHAR(120);
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while adding columns to Suppliers table: {e}")


# Insert Artist data to Suppliers table
insert_query = """
INSERT INTO Suppliers (
chinook_artist_id, CompanyName)
VALUES (?, ?)
"""
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, artist_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while inserting artist data: {e}")
print("Artist data inserted successfully")


# Retrieve data from Chinook Track table
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT TrackId, Name, GenreId, UnitPrice, ArtistId
        FROM Track t JOIN Album ab ON t.AlbumId = ab.AlbumId
        """
    )
    track_data = cursor.fetchall()
print("Track data retrieved")

# Add temporary column to Northwind Products table
# Transform columns to avoid data truncation
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Products
            ADD chinook_artist_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Products
            ADD chinook_genre_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Products
            ADD chinook_track_id INT NULL;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Products
            ALTER COLUMN ProductName NVARCHAR(200);
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while adding columns to Products table: {e}")


# Insert track data into Products table
insert_query = """
INSERT INTO Products (
chinook_track_id, ProductName, chinook_genre_id, UnitPrice, chinook_artist_id)
VALUES (?, ?, ?, ?, ?)
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, track_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while inserting track data: {e}")

# Update SupplierID column
update_query = """
UPDATE p
SET p.SupplierID = s.SupplierID
FROM Products p
INNER JOIN Suppliers s
    ON p.chinook_artist_id = s.chinook_artist_id
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(update_query)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while updating SupplierID in Product table: {e}")

# Update CategoryID column
update_query = """
UPDATE p
SET p.CategoryID = c.CategoryID
FROM Products p
INNER JOIN Categories c
    ON p.chinook_genre_id = c.chinook_genre_id
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(update_query)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while updating CategoryID in Product table: {e}")
print("Track data inserted successfully")


# Retrieve data from Chinook InvoiceLine table
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT InvoiceId, TrackId, UnitPrice, Quantity
        FROM InvoiceLine
        """
    )
    invoiceline_data = cursor.fetchall()
print("Invoice Line data retrieved")

# Create temporary table to store InvoiceLine data
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            CREATE TABLE OrderDetailsTemp (
            chinook_invoice_id INT NOT NULL,
            chinook_track_id INT NOT NULL,
            UnitPrice MONEY NOT NULL,
            Quantity INT NOT NULL
            );
            """
        )
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while creating a temporary table: {e}")


# Insert InvoiceLine data into Northwind OrderDetailsTemp table
insert_query = """
INSERT INTO OrderDetailsTemp (
chinook_invoice_id, chinook_track_id, UnitPrice, Quantity)
VALUES (?, ?, ?, ?)
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.executemany(insert_query, invoiceline_data)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while inserting invoice line data into temporary table: {e}")


# Insert corresponding data into Order Details table
insert_query = """
INSERT INTO [Order Details] (
OrderID, ProductID, UnitPrice, Quantity)
SELECT o.OrderID, p.ProductID, odt.UnitPrice, odt.Quantity
FROM OrderDetailsTemp odt 
INNER JOIN Orders o 
ON odt.chinook_invoice_id = o.chinook_invoice_id
INNER JOIN Products p
ON odt.chinook_track_id = p.chinook_track_id
"""

try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(insert_query)
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while inserting data into Order details: {e}")
print("Invoice line data inserted successfully")

# Drop temporary columns
try:
    with sql_server_conn.cursor() as sql_cursor:
        sql_cursor.execute(
            """
            ALTER TABLE Employees
            DROP COLUMN chinook_emp_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Employees
            DROP COLUMN chinook_reports_to_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Orders
            DROP COLUMN chinook_invoice_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Orders
            DROP COLUMN chinook_support_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Categories
            DROP COLUMN chinook_genre_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Suppliers
            DROP COLUMN chinook_artist_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Products
            DROP COLUMN chinook_artist_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Products
            DROP COLUMN chinook_genre_id;
            """
        )

        sql_cursor.execute(
            """
            ALTER TABLE Products
            DROP COLUMN chinook_track_id;
            """
        )

        sql_cursor.execute(
            """
            DROP TABLE OrderDetailsTemp;
            """
        )
        sql_server_conn.commit()
except Exception as e:
    sql_server_conn.rollback()
    print(f"Transaction Failed while dropping temporary columns: {e}")
