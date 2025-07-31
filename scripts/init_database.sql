*/
  =============================================================================================
  Create Database and Schemas
  =============================================================================================

  Script Purpose:
  This Script creates a new database named 'DataWarehouse' after checking if it is alerady exists.
  If the database exists, it is droppped and recreated .Additionally , the script sets up threee 
  schemas within the database: 'broze', 'silver'  and 'gold'.


  WARNING: 
  running this script will drop the entire 'DataWarehouse' database if exists.
  All data in the database will be permanently deleted. Proceed with caution 
  and ensure you have proper backups before running the script.


  */





USE master;
GO

--Drop ansd recreate the 'DataWarehouse' database


IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
  BEGIN 
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWAREHOUSE;
END;
GO
  
  
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
--GO: seperate the batches when working with multiple SQL statements.
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
