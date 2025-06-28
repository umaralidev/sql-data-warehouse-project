/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Purpose:
    This script creates views in the Gold layer of the data warehouse.
    The Gold layer contains final dimension and fact tables (Star Schema).

    Each view transforms and combines data from the Silver layer
    to create clean, enriched, and ready-to-use datasets for the business.

Usage:
    - You can use these views directly for analytics and reporting tasks.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER ( ORDER BY cust_id) AS customer_key,
ci.cust_id AS customer_id,
ci.cust_key AS customer_number,
ci.cust_firstname AS first_name,
ci.cust_lastname AS last_name,
la.centry AS country,
ci.cust_material_status AS marital_status,
CASE	WHEN ci.cust_gender != 'n/a' THEN ci.cust_gender
		ELSE COALESCE(ca.gen, 'n/a')
END as gender,
ca.bdate AS birthdate,
ci.cust_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca on ci.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la on ci.cust_key=la.cid;

SELECT * FROM gold.dim_customers

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (order by pd.prod_start_date,pd.prod_key) AS product_key,
	pd.prod_id AS product_id,
	pd.prod_key AS product_number,
	pd.prod_num AS product_name,
	pd.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pd.prod_cost AS cost,
	pd.prod_line AS product_line,
	pd.prod_start_date AS start_date
from silver.crm_prod_info pd
LEFT JOIN silver.erp_px_cat_g1v2 pc on pd.cat_id = pc.id
WHERE pd.prod_end_date is null;

SELECT * FROM gold.dim_products;

/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
	
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sales_ord_num AS order_number,
dp.product_key,
dc.customer_key,
sd.sales_order_date AS order_date,
sd.sales_ship_date AS shipping_date,
sd.sales_due_date AS due_date,
sd.sales_sales AS sales_amount,
sd.sales_qunatity AS qunatity,
sd.sales_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp on sd.sales_prod_key = dp.product_number 
LEFT JOIN gold.dim_customers dc on sd.sales_cust_id = dc.customer_id;
