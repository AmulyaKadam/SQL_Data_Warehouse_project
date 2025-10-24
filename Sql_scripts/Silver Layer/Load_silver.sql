/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE sp_load_silver_layer()
BEGIN
    DECLARE startBatchTime DATETIME DEFAULT NOW();
    DECLARE endBatchTime DATETIME;
    DECLARE startTime DATETIME;
    DECLARE endTime DATETIME;
    DECLARE err_msg TEXT DEFAULT '';

    -- This acts like TRY / EXCEPT
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET err_msg = CONCAT('Error occurred during ELT process at: ', NOW());
        SELECT err_msg AS error_message;
        SET endBatchTime = NOW();
        SELECT endBatchTime AS endBatchTime,
               TIMEDIFF(endBatchTime, startBatchTime) AS totalBatchDuration,
               'Process terminated due to error' AS message;
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT startBatchTime AS startBatchTime, 'Starting ELT process for Silver layer' AS message;

    -- ===========================================
    -- CRM CUSTOMER INFO
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading silver.crm_cust_info' AS message;

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_marital_status, -- Normalize marital status values to readable format
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gndr, -- Normalize gender values to readable format
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
        FROM bronze.crm_cust_info
        WHERE cst_id > 0
    ) t
    WHERE rn = 1; -- Select the most recent record per customer

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading silver.crm_cust_info' AS message;


    -- ===========================================
    -- CRM PRODUCT INFO
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading silver.crm_prd_info' AS message;

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Extract category ID
        SUBSTRING(prd_key,7,CHAR_LENGTH(prd_key))  AS prd_key,  -- Extract product key
        prd_nm,
        prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sale'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line, -- Map product line codes to descriptive values
        prd_start_dt,
        DATE_SUB(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS prd_end_dt -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading silver.crm_prd_info' AS message;


    -- ===========================================
    -- CRM SALES DETAILS
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading silver.crm_sales_details' AS message;

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL 
             ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR),'%Y%m%d') END sls_order_dt,
        CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL 
             ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR),'%Y%m%d') END sls_ship_dt,
        CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL 
             ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR),'%Y%m%d') END sls_due_dt,
        CASE 
            WHEN s.sls_sales IS NULL OR s.sls_sales <= 0 
                 OR s.sls_sales != s.sls_quantity * ABS(s.fixed_price)
            THEN s.sls_quantity * ABS(s.fixed_price)
            ELSE s.sls_sales 
        END sls_sales, -- Recalculate sales if original value is missing or incorrect
        s.sls_quantity,
        s.fixed_price
    FROM (
        SELECT *,
               CASE 
                   WHEN sls_price IS NULL OR sls_price <= 0 
                   THEN sls_sales / NULLIF(sls_quantity, 0)
                   ELSE sls_price 
               END AS fixed_price -- Derive price if original value is invalid
        FROM bronze.crm_sales_details
    ) AS s;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading silver.crm_sales_details' AS message;


    -- ===========================================
    -- ERP CUST AZ12
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading silver.erp_cust_az12' AS message;

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END cid, -- Remove 'NAS' prefix if present
        CASE
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END bdate,  -- Set future birthdates to NULL
        CASE
            WHEN gen IS NULL THEN 'n/a'
            WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\t',''), '\n',''), '\r',''), '\u00A0','')) = '' THEN 'n/a'
            WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\t',''), '\n',''), '\r',''), '\u00A0',''))) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\t',''), '\n',''), '\r',''), '\u00A0',''))) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END gen -- Normalize gender values and handle unknown cases
    FROM bronze.erp_cust_az12;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading silver.erp_cust_az12' AS message;


    -- ===========================================
    -- ERP LOC A101
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading silver.erp_loc_a101' AS message;

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101(cid,cntry)
    SELECT 
        TRIM(REPLACE(cid,'-','')) AS cid,
        CASE
            WHEN cntry IS NULL THEN 'n/a'
            WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\t',''), '\n',''), '\r',''), '\u00A0','')) = '' THEN 'n/a'
            WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\t',''), '\n',''), '\r',''), '\u00A0','')) IN ('US','USA') THEN 'United States'
            WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\t',''), '\n',''), '\r',''), '\u00A0','')) IN ('DE') THEN 'Germany'
            ELSE TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\t',''), '\n',''), '\r',''), '\u00A0',''))
        END cntry -- Normalize and Handle missing or blank country codes
    FROM bronze.erp_loc_a101;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading silver.erp_loc_a101' AS message;


    -- ===========================================
    -- ERP PX CAT G1V2
    -- ===========================================
    SET startTime = NOW();
    SELECT startTime AS startTime, 'Truncating and loading silver.erp_px_cat_g1v2' AS message;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,MAINTENANCE)
    SELECT id, cat, subcat, MAINTENANCE
    FROM bronze.erp_px_cat_g1v2;

    SET endTime = NOW();
    SELECT endTime AS endTime, TIMEDIFF(endTime, startTime) AS duration, 'Completed loading silver.erp_px_cat_g1v2' AS message;


    -- End of ELT Process
    SET endBatchTime = NOW();
    SELECT endBatchTime AS endBatchTime,
           TIMEDIFF(endBatchTime, startBatchTime) AS totalBatchDuration,
           'ELT process completed successfully for Silver layer' AS message;

    COMMIT;
END$$

DELIMITER ;
CALL sp_load_silver_layer();


