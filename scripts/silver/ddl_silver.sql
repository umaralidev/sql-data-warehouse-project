IF OBJECT_ID ('silver.crm_cust_info', 'U') is NOT NULL
	DROP TABLE silver.crm_cust_info;
GO
  
CREATE TABLE silver.crm_cust_info (
	cust_id					INT,
	cust_key				NVARCHAR (50),
	cust_firstname			NVARCHAR (50),
	cust_lastname			NVARCHAR (50),
	cust_material_status	NVARCHAR(50),
	cust_gender				NVARCHAR (50),
	cust_create_date		DATE,
	dwh_create_date			DATETIME2 DEFAULT GETDATE()
);
GO
  
IF OBJECT_ID ('silver.crm_prod_info', 'U') is NOT NULL
	DROP TABLE silver.crm_prod_info;
GO
  
CREATE TABLE silver.crm_prod_info (
	prod_id					INT,
	cat_id					NVARCHAR (50),
	prod_key				NVARCHAR (50),
	prod_num				NVARCHAR (50),
	prod_cost				INT,
	prod_line				NVARCHAR (50),
	prod_start_date			DATE,
	prod_end_date			DATE,
	dwh_create_date			DATETIME2 DEFAULT GETDATE()
);
GO
  
IF OBJECT_ID ('silver.crm_sales_details', 'U') is NOT NULL
	DROP TABLE silver.crm_sales_details;
GO
  
CREATE TABLE silver.crm_sales_details(
	sales_ord_num			NVARCHAR (50),
	sales_prod_key			NVARCHAR (50),
	sales_cust_id			INT,
	sales_order_date		DATE,
	sales_ship_date			DATE,
	sales_due_date			DATE,
	sales_sales				INT ,
	sales_qunatity			INT,
	sales_price				INT,
	dwh_create_date			DATETIME2 DEFAULT GETDATE()
);
GO
  
IF OBJECT_ID ('silver.erp_loc_a101', 'U') is NOT NULL
	DROP TABLE silver.erp_loc_a101;
GO
  
CREATE TABLE silver.erp_loc_a101 (
	cid		NVARCHAR(50),
	centry	NVARCHAR(50),
	dwh_create_date			DATETIME2 DEFAULT GETDATE()
);
GO
  
IF OBJECT_ID ('silver.erp_cust_az12', 'U') is NOT NULL
	DROP TABLE silver.erp_cust_az12;
GO
  
CREATE TABLE silver.erp_cust_az12(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50),
	dwh_create_date			DATETIME2 DEFAULT GETDATE()
);
GO
  
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') is NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
GO
  
CREATE TABLE silver.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance	NVARCHAR(50),
	dwh_create_date			DATETIME2 DEFAULT GETDATE()
);
