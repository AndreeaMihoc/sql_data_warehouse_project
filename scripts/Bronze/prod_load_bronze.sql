/*
Purpose: 
 This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
*/

create or alter procedure  bronze.load_bronze as

begin
	declare @start_time as datetime, @end_time as datetime
	set @start_time = GETDATE();
	begin try
	set @start_time = GETDATE();
		truncate table [bronze].[crm_cust_info]
		bulk insert [bronze].[crm_cust_info]
		from 'C:\Users\pandi\Documents\1. Portfolio\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator =',',
		tablock
		);
	set @end_time = GETDATE();
	print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

	set @start_time = GETDATE();
		truncate table [bronze].[crm_prd_info]
		bulk insert [bronze].[crm_prd_info]
		from 'C:\Users\pandi\Documents\1. Portfolio\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator =',',
		tablock
		);
	set @end_time = GETDATE();
	print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

	set @start_time = getdate();
		truncate table [bronze].[crm_sales_details]
		bulk insert [bronze].[crm_sales_details]
		from 'C:\Users\pandi\Documents\1. Portfolio\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator =',',
		tablock
		);
	set @end_time = GETDATE();
	print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

	set @start_time = GETDATE();
		truncate table [bronze].[erp_cust_az12]
		bulk insert [bronze].[erp_cust_az12]
		from 'C:\Users\pandi\Documents\1. Portfolio\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,
		fieldterminator =',',
		tablock
		);
	set @end_time = GETDATE();
	print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

	set @start_time = GETDATE();
		truncate table [bronze].[erp_loc_a101]
		bulk insert [bronze].[erp_loc_a101]
		from 'C:\Users\pandi\Documents\1. Portfolio\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,
		fieldterminator =',',
		tablock
		);
	set @end_time = GETDATE();
	print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

	set @start_time = GETDATE()
		truncate table [bronze].[erp_px_cat_g1v2]
		bulk insert [bronze].[erp_px_cat_g1v2]
		from 'C:\Users\pandi\Documents\1. Portfolio\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
	set @end_time = GETDATE();
	print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
	end try
	begin catch
		print '==========================================='
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message' + error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '==========================================='
	end catch
set @end_time = GETDATE();
PRINT 'Total  Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
end
