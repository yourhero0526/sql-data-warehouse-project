/*
===================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===================================================================================
Script Purpose:
This script performs the ETL (Extract, Transform, Load) process from the bronze layer to the silver layer.
It primarily eliminates all unwanted spaces, creates new columns to link multiple tables for the gold layer.
It first truncates all data inside the silver layer, then inserts performs all ETL processes again.

	Parameters:
	None.
	This stored procedure does not accept any parameters or return any value.

	Usage Example:
		EXEC silver.load_silver;
===================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '==========================================================================';
	PRINT 'Loading: Silver (T2) Layer';
	PRINT '==========================================================================';

	PRINT '--------------------------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '--------------------------------------------------------------------------';

		


	/* 
	===================================================================================
	1. TRUNCATE & INSERT INTO silver.crm_cust_info A.K.A. CRM - Customer Information
	===================================================================================
	*/
	
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT '>> Inserting Data Into: silver.crm_cust_info';

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname, -- Example: TRIMMING
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- Example: DATA NORMALIZATION
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'Unknown'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'Unknown' -- Example: Handling missing data
		END cst_gndr,
		cst_create_date

	-- Example: Removing duplicates
	FROM ( 
	SELECT
		*, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t WHERE flag_last = 1 

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ========================================================================';

	-- usually, find the highest value using a timestamp or date. 



	/* 
	===================================================================================
	2. TRUNCATE & INSERT INTO silver.crm_prd_info A.K.A. CRM - Product Information
	===================================================================================
	*/

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	PRINT '>> Inserting Data Into: silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,	-- Extract Category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,			-- Extract product key
		TRIM(prd_nm) AS prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'S' THEN 'Sales'
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			ELSE 'Unknown'
		END AS prd_line, -- Map product line codes to descriptive values
		CAST (prd_start_dt AS DATE) AS prd_start_dt, -- converting data type to another
		CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt -- data enrichment, add new value
	FROM bronze.crm_prd_info
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ========================================================================';

	/* 
	===================================================================================
	3. TRUNCATE & INSERT INTO silver.crm_sales_details A.K.A. CRM - Sales Details
	===================================================================================
	*/

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	PRINT '>> Inserting Data Into: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)

	SELECT 
		UPPER(TRIM(sls_ord_num)) AS sls_ord_num,
		UPPER(TRIM(sls_prd_key)) AS sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL --Data Type Casting
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		-- Data Type Casting: Changing type from INT to DATE
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		-- Handling Invalid Data
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR (sls_sales != sls_quantity * sls_price) 
			THEN ABS(sls_quantity) * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,

		-- Handling Invalid Data
		CASE WHEN sls_quantity IS NULL OR sls_quantity <= 0
			THEN ABS(sls_sales) / ABS(sls_price)
			ELSE sls_quantity
		END AS sls_quantity,

		-- Handling Invalid Data
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN ABS(sls_sales) / ABS(sls_quantity)
			ELSE sls_price
		END AS sls_price

	FROM bronze.crm_sales_details

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ========================================================================';

	/* 
	===================================================================================
	4. TRUNCATE & INSERT INTO silver.erp_cust_az12 A.K.A. ERP - Additional Customer Information (Birthdate)
	===================================================================================
	*/

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>> Inserting Data Into: silver.erp_cust_az12';


	INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)

	SELECT

		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,

		CASE WHEN bdate > GETDATE() THEN NULL
			WHEN bdate < '1900-01-01' THEN NULL
			ELSE bdate
		END AS bdate,

		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'Unknown'
		END AS gen
	FROM bronze.erp_cust_az12

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ========================================================================';
	/* 
	===================================================================================
	5. TRUNCATE & INSERT INTO silver.erp_loc_a101 A.K.A. ERP - Additional Customer Information (Country)
	===================================================================================
	*/

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT '>> Inserting Data Into: silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)

	SELECT 
	REPLACE(cid, '-', '') cid,

	CASE WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
		WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA') THEN 'Australia'
		WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM', 'UK') THEN 'United Kingdom'
		WHEN UPPER(TRIM(cntry)) IN ('CANADA') THEN 'Canada'
		WHEN UPPER(TRIM(cntry)) IN ('FRANCE') THEN 'France'
		WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'Unknown'
		ELSE TRIM(cntry)
	END AS cntry

	FROM bronze.erp_loc_a101

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ========================================================================';


	/* 
	===================================================================================
	6. TRUNCATE & INSERT INTO silver.erp_px_cat_g1v2 A.K.A. ERP - Product Categories
	===================================================================================
	*/

	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)

	SELECT
		CASE WHEN UPPER(TRIM(id)) = 'CO_PD' THEN 'CO_PE'
			ELSE UPPER(TRIM(id))
		END AS id,
	
		TRIM(cat) AS cat,
		TRIM(subcat) AS subcat,
	
		CASE WHEN UPPER(TRIM(maintenance)) IN ('Y', 'YES') THEN 'Yes'
			WHEN UPPER(TRIM(maintenance)) IN ('N', 'NO') THEN 'No'
			ELSE 'Unknown'
		END AS maintenance

	FROM bronze.erp_px_cat_g1v2

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> ========================================================================';

	SET @batch_end_time = GETDATE();
	PRINT '==========================================================================';
	PRINT 'Loading the Silver (T2) Layer is Complete.';
	PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '==========================================================================';

	END TRY
	
	BEGIN CATCH
		PRINT '==========================================================================';
		PRINT 'ERROR OCCURRED WHILE LOADING THE BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================================';
	END CATCH
END
