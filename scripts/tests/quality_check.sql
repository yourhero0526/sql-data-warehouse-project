-- Check for nulls, duplicates in Primary Key
-- Expectation: No Result

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

/*

=====================================================
Codes beneath this comment will all be about testing & checking data quality
=====================================================

*/

-- 1. Check Quality of Bronze Layer
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- 2. Check for unwanted spaces
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- 3. Data Standardization & Consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info
-- Note: we will change this into full values rather than abbreviated terms

-- 4. Check Quality of crm_cust_info
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

/* 
=====================================================================
CLEANSING & TRANSFORMATION CODE FOR: silver.crm_prd_info
=====================================================================
*/

-- 1. Check for nulls or duplicates for product id
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- 2. Check if prd_nm needs data normalizatino
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
	
-- 3. Check if prd_cost has negative numbers or NULLS
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- 4. Check the distinctions of prd_line
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- 5. Check Invalid Order of Dates
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- 6. Check Quality of silver.crm_prd_info
SELECT prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info

-- 7. Select Date Quality of bronze.crm_sales_details
SELECT
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

-- 8. Check sales integer
SELECT 

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR (sls_sales != sls_quantity * sls_price)
	THEN ABS(sls_quantity) * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_quantity IS NULL OR sls_quantity <= 0
	THEN ABS(sls_sales) / ABS(sls_price)
	ELSE sls_quantity
END AS sls_quantity,

CASE WHEN sls_price IS NULL OR sls_price <= 0
	THEN ABS(sls_sales) / ABS(sls_quantity)
	ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- 9. Check Quality of silver.crm_sales_details
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price

-- 10. Check Quality of silver.erp_cust_az12
-- a. cid
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,

bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

SELECT * FROM silver.crm_cust_info

-- b. bdate
SELECT
bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
ORDER BY bdate DESC

-- c. gen
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'Unknown'
END AS gen
FROM bronze.erp_cust_az12

-- 11. Check Quality of bronze.erp_loc_a101
-- a. cid

SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info) 

-- b. cntry
 
SELECT DISTINCT
cntry,
CASE WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'USA'
	WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA') THEN 'Australia'
	WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM', 'UK') THEN 'United Kingdom'
	WHEN UPPER(TRIM(cntry)) IN ('CANADA') THEN 'Canada'
	WHEN UPPER(TRIM(cntry)) IN ('FRANCE') THEN 'France'
	ELSE 'Unknown'
END AS cntry
FROM bronze.erp_loc_a101

-- 12. Check Quality of silver.erp_loc_a101

SELECT DISTINCT
cntry
FROM silver.erp_loc_a101

SELECT
cid
FROM silver.erp_loc_a101
WHERE cid LIKE 'NAS%' OR LEN(cid) != 10

SELECT * FROM silver.erp_loc_a101

-- 13. Check Quality of bronze.erp_px_cat_g1v2

-- a. id
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)

SELECT *
FROM silver.crm_prd_info
WHERE cat_id NOT IN (SELECT id FROM bronze.erp_px_cat_g1v2)

-- b. cat
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

-- c. subcat

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

-- d. maintenance
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

-- 14. Check Quality of silver.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2

SELECT 
*
FROM silver.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id from silver.crm_prd_info)

SELECT DISTINCT
maintenance 
FROM silver.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)
