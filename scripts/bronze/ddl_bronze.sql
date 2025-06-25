use DataWarehouse;

IF OBJECT_ID ('bronze.crm_cust_info', 'U') is NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cust_id					INT,
	cust_key				NVARCHAR (50),
	cust_firstname			NVARCHAR (50),
	cust_lastname			NVARCHAR (50),
	cust_material_status	NVARCHAR(50),
	cust_gender				NVARCHAR (50),
	cust_create_date		DATE
);

IF OBJECT_ID ('bronze.crm_prod_info', 'U') is NOT NULL
	DROP TABLE bronze.crm_prod_info;
CREATE TABLE bronze.crm_prod_info (
	prod_id					INT,
	prod_key				NVARCHAR (50),
	prod_num				NVARCHAR (50),
	prod_cost				INT,
	prod_line				NVARCHAR (50),
	prod_start_date			DATETIME,
	prod_end_date			DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') is NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sales_ord_num			NVARCHAR (50),
	sales_prod_key			NVARCHAR (50),
	sales_cust_id			INT,
	sales_order_date		INT,
	sales_ship_date			INT,
	sales_due_date			INT,
	sales_sales				INT ,
	sales_qunatity			INT,
	sales_price				INT 
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') is NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid		NVARCHAR(50),
	centry	NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') is NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') is NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance	NVARCHAR(50)
);
