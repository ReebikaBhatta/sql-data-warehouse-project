
--Create stored procedure
-- Save frequently used SQL code in stored procedure in database 
-- sql runs the try block, and if it falls , it runs the CATCH block to handle the error.
--track ETL Duration: Helps to identify bottlenecks , optimize performance, monitor trends, detect issues.
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


USE DataWarehouse;


EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '===========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '===========================================';
        
            PRINT '________________________________________';
            PRINT 'Loading CRM Tables'
            PRINT '________________________________________';



            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_cust_info_staging;

            BULK INSERT bronze.crm_cust_info_staging
            FROM 'C:\Users\bhatt\Downloads\cust_info_cleaned.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                TABLOCK,
                CODEPAGE = '65001',
                DATAFILETYPE = 'char'
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

       
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_prd_info;
            BULK INSERT bronze.crm_prd_info
            FROM 'C:\Users\bhatt\Downloads\prd_info.csv'

            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';


        
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_sales_details;
            BULK INSERT bronze.crm_sales_details
            FROM 'C:\Users\bhatt\Downloads\sales_details.csv'

            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';


      
        
            PRINT '________________________________________';
            PRINT 'Loading ERP Tables'
            PRINT '________________________________________';

            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.epr_cust_az12;
            BULK INSERT bronze.epr_cust_az12
            FROM 'C:\Users\bhatt\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';


       
            SET @start_time = GETDATE();
            TRUNCATE TABLE  bronze.epr_LOC_a101;
            BULK INSERT bronze.epr_LOC_a101
            FROM 'C:\Users\bhatt\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';


            SET @start_time = GETDATE();

            TRUNCATE TABLE  bronze.epr_px_cat_g1v2;
            BULK INSERT bronze.epr_px_cat_g1v2
            FROM 'C:\Users\bhatt\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        END TRY
        BEGIN CATCH
        PRINT '================================================'
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '=================================================='
        END CATCH
        SET @batch_end_time = GETDATE();
        PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';

      

END
