
# Chinook to Northwind Data Migration

This project demonstrates the migration of data from the Chinook database (MySQL server) to the Northwind database (MS SQL server). The migration process covers multiple tables, including employees, customers, orders (invoices), genres, artists, tracks (products), and invoice lines (order details). 

## Features
- **MySQL to MS SQL migration**: Uses `pymysql` and `pyodbc` to handle connections to both databases.
- **Tables migrated**: 
  - Chinook `Employees` -> Northwind `Employees`
  - Chinook `Customers` -> Northwind `Customers`
  - Chinook `Invoice` -> Northwind `Orders`
  - Chinook `InvoiceLine` -> Northwind `Order Details`
  - Chinook `Genre` -> Northwind `Categories`
  - Chinook `Artist` -> Northwind `Suppliers`
  - Chinook `Track` -> Northwind `Products`

## Key Challenges and Solutions
### 1. Migrating `InvoiceLine` to `Order Details`:
- **Issue**: Northwind’s `Order Details` table uses a composite key consisting of `OrderID` and `ProductID`, making direct migration impossible.
- **Solution**: A temporary table (`OrderDetailsTemp`) was created in Northwind to store the intermediate data from `InvoiceLine`. After inserting data into this table, a subsequent query matched the correct `OrderID` and `ProductID` values before inserting the data into the final `Order Details` table.

### 2. Handling `ReportsTo` column in `Employees` table:
- **Issue**: The `ReportsTo` column in Chinook's `Employee` table references other employees, so the corresponding IDs needed to be updated in the Northwind `Employees` table.
- **Solution**: Two temporary columns were added to the `Employees` table in Northwind to store the Chinook IDs. After inserting the data, an update query was used to map the `ReportsTo` values by joining the temporary columns with the new Northwind employee IDs.

### 3. Transforming Customer Data:
- **Issue**: Chinook separates first and last names, while Northwind uses a single `ContactName` field.
- **Solution**: The `CONCAT` function was used to combine `FirstName` and `LastName` into the `ContactName` field during the migration process.

### 4. Adjusting Column Sizes:
- **Issue**: Several columns in Northwind tables had different size limitations compared to their Chinook counterparts, causing potential data truncation issues.
- **Solution**: The columns in Northwind tables were altered (e.g., `NVARCHAR(80)`) to match the data requirements of the corresponding columns from Chinook.

### 5. Migrating Tracks to Products:
- **Issue**: Tracks in Chinook are related to albums and genres, which needed to be mapped to products in Northwind.
- **Solution**: Temporary columns were added to the `Products` table to store `chinook_artist_id`, `chinook_genre_id`, and `chinook_track_id`. Once data was inserted, update queries were run to map these to the appropriate `SupplierID` and `CategoryID`.

## How to Run
1. Clone the repository.
2. Install required dependencies:
    ```bash
    pip install pymysql pyodbc
    ```
3. Set up MySQL and MS SQL servers and update connection details in the script.
4. Run the migration script.

## Dependencies
- `pymysql`: For MySQL database interaction.
- `pyodbc`: For MS SQL Server interaction.

## Conclusion
This project handles the complex migration of data between two different database systems with various table structures and constraints. Challenges such as composite keys, foreign key relationships, and data size differences were solved using temporary tables, data transformations, and careful planning of the migration process.
