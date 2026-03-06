/*

===================================================================
Create Database & Schemas
===================================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If yes, it is dropped and recreated. Additionally, the script sets up three schema
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backup before running the script.
*/

-- Create Database 'Datawarehouse'

USE master;
GO

-- Drop & recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "DataWarehouse")
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO
  
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
