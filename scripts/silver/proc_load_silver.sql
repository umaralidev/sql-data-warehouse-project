EXECUTE silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME;
	BEGIN TRY
		PRINT '=========================================================='
		PRINT 'Loading Silver Layer'
		PRINT '=========================================================='

		PRINT '----------------------------------------------------------'
		PRINT 'Loading CRM TABLES'
		PRINT '----------------------------------------------------------'

		SET @start_time =GETDATE();
	-- Clean and Load into SIlver.crm_cust_info
		Print '>> Truncating Table : silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> INSERTING Data into : silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
		cust_id,
		cust_key,
		cust_firstname,
		cust_lastname,
		cust_material_status,
		cust_gender,
		cust_create_date)

		SELECT
		cust_id,
		cust_key,
		TRIM(cust_firstname) AS cust_firstname ,
		TRIM(cust_lastname) AS cust_lastname,

		CASE	WHEN UPPER(TRIM(cust_material_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cust_material_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
		END cust_material_status,

		CASE	WHEN UPPER(TRIM(cust_gender)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
				ELSE ' n/a'
		END cust_gender,
		cust_create_date
		FROM (
			SELECT  *,
			ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cust_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cust_id IS NOT NULL
		)t WHERE flag_last =1 ;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		SET @start_time = GETDATE();
		-- Clean and Load into silver.crm_prod_info
		Print '>> Truncating Table : silver.crm_prod_info'
		TRUNCATE TABLE silver.crm_prod_info;
		PRINT '>> INSERTING Data into : silver.crm_prod_info'

		INSERT INTO silver.crm_prod_info(
			prod_id,
			cat_id,
			prod_key,
			prod_num,
			prod_cost,
			prod_line,
			prod_start_date,
			prod_end_date
		)
		SELECT prod_id,
		REPLACE(SUBSTRING(prod_key,1,5), '-', '_') AS cat_id,	-- Extrct Cat_id
		SUBSTRING(prod_key,7,len(prod_key)) As prod_key,		-- Extract Prod_key
		prod_num,												--Handle null values
		ISNULL(prod_cost,0) AS prod_cost,
		CASE	UPPER(TRIM(prod_line))
				WHEN  'M' THEN 'Mountain'
				WHEN  'R' THEN 'Road'
				WHEN  'S' THEN 'Other  Sales'
				WHEN  'T' THEN 'Touring'
				ELSE 'n/a'
		END AS prod_line,
		CAST(prod_start_date AS DATE) AS prod_start_date,
		CAST(LEAD(prod_start_date) OVER (PARTITION BY prod_key ORDER BY prod_start_date)-1 AS DATE) AS prod_end_date
		FROM bronze.crm_prod_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		SET @start_time =GETDATE();
		-- Clean and Load into silver.crm_sales_details

		Print '>> Truncating Table : silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> INSERTING Data into : silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sales_ord_num,
			sales_prod_key,
			sales_cust_id,
			sales_order_date,
			sales_ship_date,
			sales_due_date,
			sales_sales,
			sales_qunatity,
			sales_price
		)
		SELECT 
		sales_ord_num,
		sales_prod_key,
		sales_cust_id,
		CASE 
			WHEN sales_order_date = 0 OR LEN(sales_order_date) != 8 THEN NULL
			ELSE CAST(CAST(sales_order_date AS varchar) AS DATE)
		END AS sales_order_date,

		CASE 
			WHEN sales_ship_date = 0 OR LEN(sales_ship_date) != 8 THEN NULL
			ELSE CAST(CAST(sales_ship_date AS varchar) AS DATE)
		END AS sales_ship_date,

		CASE 
			WHEN sales_due_date = 0 OR LEN(sales_due_date) != 8 THEN NULL
			ELSE CAST(CAST(sales_due_date AS varchar) AS DATE)
		END AS sales_due_date,

		CASE WHEN sales_sales IS NULL OR sales_sales <=0 OR sales_sales != sales_qunatity * ABS(sales_price)
				THEN sales_qunatity * ABS(sales_price)
			ELSE sales_sales
		END AS sales_sales,

		sales_qunatity,

		CASE WHEN sales_price IS NULL OR sales_price <=0 
				THEN sales_sales / NULLIF(sales_qunatity,0)
			ELSE sales_price
		END AS sales_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		--ERP DATA INGESTIONS
		PRINT '----------------------------------------------------------'
		PRINT 'Loading ERP TABLES'
		PRINT '----------------------------------------------------------'

		SET @start_time =GETDATE();

		-- Clean and load erp_cust_az12
		Print '>> Truncating Table : silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> INSERTING Data into : silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
				ELSE cid
		END AS cid,

		CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
		END AS bdate,

		CASE	WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'

		SET @start_time =GETDATE();
		-- clean and load erp_loc_a101
		
		Print '>> Truncating Table : silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> INSERTING Data into : silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
		cid,centry)
		SELECT 
		REPLACE(cid,'-','') cid,
		CASE 
			WHEN TRIM(centry) = 'DE' THEN 'Germany'
			WHEN TRIM(centry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(centry) IS NULL OR TRIM(centry) = '' THEN 'n/a'
			ELSE TRIM(centry)
		END AS centry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		SET @start_time = GETDATE();
		-- clean and load erp_px_cat_g1v2


		Print '>> Truncating Table : silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> INSERTING Data into : silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================================='
	END CATCH
END
