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

DELIMITER $$

CREATE PROCEDURE bronze.load_bronze()
BEGIN
    DECLARE startBatchTime DATETIME DEFAULT NOW();
    DECLARE endBatchTime DATETIME;
    DECLARE startTime DATETIME;
    DECLARE endTime DATETIME;
    DECLARE err_msg TEXT DEFAULT '';

    -- Try/Except-like handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET err_msg = CONCAT('Error occurred during Bronze layer load at: ', NOW());
        SELECT err_msg AS error_message;
        SET endBatchTime = NOW();
        SELECT endBatchTime AS endBatchTime,
               TIMEDIFF(endBatchTime, startBatchTime) AS totalBatchDuration,
               'Process terminated due to error' AS message;
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT startBatchTime AS startBatchTime, 'Starting Bronze layer load process' AS message;

    -- Preserve current SQL mode
    SET @OLD_SQL_MODE = @@SQL_MODE;
    SET SQL_MODE = '';

    -- ===========================================
    -- CRM CUSTOMER INFO
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading bronze.crm_cust_info' AS message;

    TRUNCATE TABLE bronze.crm_cust_info;
    LOAD DATA INFILE 'D:\\Projects\\Data_warehouse_sql\\dataset\\source_crm\\cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading bronze.crm_cust_info' AS message;


    -- ===========================================
    -- CRM PRODUCT INFO
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading bronze.crm_prd_info' AS message;

    TRUNCATE TABLE bronze.crm_prd_info;
    LOAD DATA INFILE 'D:\\Projects\\Data_warehouse_sql\\dataset\\source_crm\\prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading bronze.crm_prd_info' AS message;


    -- ===========================================
    -- CRM SALES DETAILS
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading bronze.crm_sales_details' AS message;

    TRUNCATE TABLE bronze.crm_sales_details;
    LOAD DATA INFILE 'D:\\Projects\\Data_warehouse_sql\\dataset\\source_crm\\sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading bronze.crm_sales_details' AS message;


    -- ===========================================
    -- ERP CUST AZ12
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading bronze.erp_cust_az12' AS message;

    TRUNCATE TABLE bronze.erp_cust_az12;
    LOAD DATA INFILE 'D:\\Projects\\Data_warehouse_sql\\dataset\\source_erp\\CUST_AZ12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading bronze.erp_cust_az12' AS message;


    -- ===========================================
    -- ERP LOC A101
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading bronze.erp_loc_a101' AS message;

    TRUNCATE TABLE bronze.erp_loc_a101;
    LOAD DATA INFILE 'D:\\Projects\\Data_warehouse_sql\\dataset\\source_erp\\LOC_A101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading bronze.erp_loc_a101' AS message;


    -- ===========================================
    -- ERP PX CAT G1V2
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading bronze.erp_px_cat_g1v2' AS message;

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    LOAD DATA INFILE 'D:\\Projects\\Data_warehouse_sql\\dataset\\source_erp\\PX_CAT_G1V2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading bronze.erp_px_cat_g1v2' AS message;


    -- ===========================================
    -- Finalize and Commit
    -- ===========================================
    SET SQL_MODE = @OLD_SQL_MODE;
    SET endBatchTime = NOW();
    SELECT endBatchTime AS endBatchTime,
           TIMEDIFF(endBatchTime, startBatchTime) AS totalBatchDuration,
           'Bronze layer load process completed successfully' AS message;

    COMMIT;
END $$

DELIMITER ;

CALL  bronze.load_bronze();