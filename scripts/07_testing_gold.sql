-- 1. Check: Customer - cst_id
SELECT cst_id, COUNT(*) FROM
	(SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON			ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON			ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1

-- 2. Check: gndr, gen
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'Unknown')
	END AS new_gen
	FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON			ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON			ci.cst_key = la.cid
ORDER BY 1, 2

-- 3. Check Gold - Customers Quality

SELECT * FROM gold.dim_customers

SELECT distinct gender FROM gold.dim_customers

-- 4. Testing: Product

SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt, -- Removed End date because all are null, present
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn

LEFT JOIN silver.erp_px_cat_g1v2 pc
ON		pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data

-- 5. Check if there's any duplicate for prd_key
SELECT prd_key, COUNT(*) FROM (

SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt, -- Removed End date because all are null, present
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn

LEFT JOIN silver.erp_px_cat_g1v2 pc
ON		pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data
)t GROUP BY prd_key
HAVING COUNT(*) > 1

-- 6. Check Quality of Sales

SELECT * FROM gold.fact_sales

-- b. Foreign Key Integrity (dimensions)

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL

