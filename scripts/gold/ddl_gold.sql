
-- Gold layer customer Dimmension Table
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


-- Gold Layer product dimensions.
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


-- CREATE SALES FACT Table
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
