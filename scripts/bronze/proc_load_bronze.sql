CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME;
	BEGIN TRY
		PRINT '=========================================================='
		PRINT 'Loading Bronze Layer'
		PRINT '=========================================================='

		PRINT '----------------------------------------------------------'
		PRINT 'Loading CRM TABLES'
		PRINT '----------------------------------------------------------'

		SET @start_time =GETDATE();
		PRINT'>> Truncating Table : bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data into : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\DWH Projects\1. SQL Data Warehouse Project\SQL DWH Project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : bronze.crm_prod_info'
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>> Inserting Data into : bronze.crm_prod_info'
		BULK INSERT bronze.crm_prod_info
		FROM 'E:\DWH Projects\1. SQL Data Warehouse Project\SQL DWH Project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'





		SET @start_time =GETDATE();
		PRINT'>> Truncating Table : bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\DWH Projects\1. SQL Data Warehouse Project\SQL DWH Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		--ERP DATA INGESTIONS
		PRINT '----------------------------------------------------------'
		PRINT 'Loading ERP TABLES'
		PRINT '----------------------------------------------------------'

		SET @start_time =GETDATE();
		PRINT'>> Truncating Table : bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\DWH Projects\1. SQL Data Warehouse Project\SQL DWH Project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'

		SET @start_time =GETDATE();
		PRINT'>> Truncating Table : bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data into : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\DWH Projects\1. SQL Data Warehouse Project\SQL DWH Project\datasets\source_erp\LOC_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\DWH Projects\1. SQL Data Warehouse Project\SQL DWH Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
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

END;

EXECUTE bronze.load_bronze;
