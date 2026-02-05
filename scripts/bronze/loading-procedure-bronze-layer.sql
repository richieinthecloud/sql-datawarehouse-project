/*
=============================================================================
Bronze Layer - Bulk Data Load Procedure
=============================================================================
Purpose of this Script: 
  Automated stored procedure to perform bulk insert operations from CSV source files into Bronze layer staging tables. 
This procedure truncates existing data and loads fresh data from CRM and ERP systems. Includes error handling, load duration 
tracking, and detailed logging for monitoring ETL performance.

BIG WARNING:
  This procedure TRUNCATES all Bronze layer tables before loading, which permanently deletes all existing data in those tables. 
Do not run this procedure unless you are certain the source CSV files are valid and accessible. Ensure file paths are correct
before execution. Running this in production without proper validation could result in complete data loss in the Bronze layer. 
Always test in a development environment first.

Requirements: 
	Bulk insert permissions and file system access to CSV source files
*/

create or alter procedure Bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @start_batch datetime, @end_batch datetime;
	begin try
		set @start_batch = GETDATE();
		print '=============================================';
		print 'Initializing Load (Bulk Insert) Procedure into Bronze layer';
		print '=============================================';
		print '=============================================';
		print 'Initializing CRM tables';
		print '=============================================';

		set @start_time = GETDATE();
		print '>> Truncating table: Bronze.crm_cust_info';
		truncate table Bronze.crm_cust_info;

		print '>> Loading data into Bronze.crm_cust_info';
		bulk insert Bronze.crm_cust_info
		from 'C:\Users\yungr\OneDrive\Desktop\Professional Development\SQL Practice\SQL with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = GETDATE();
		print '>> crm_cust_info completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		set @start_time = GETDATE();
		print '>> Truncating Table: Bronze.crm_prd_info';
		truncate table Bronze.crm_prd_info;

		print '>> Loading data into Bronze.crm_prd_info';
		bulk insert Bronze.crm_prd_info
		from 'C:\Users\yungr\OneDrive\Desktop\Professional Development\SQL Practice\SQL with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = GETDATE();
		print '>> crm_prd_info completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		set @start_time = GETDATE();
		print '>> Truncating Table: Bronze.crm_sales_details';
		truncate table Bronze.crm_sales_details;

		print '>> Loading data into Bronze.crm_sales_details';
		bulk insert Bronze.crm_sales_details
		from 'C:\Users\yungr\OneDrive\Desktop\Professional Development\SQL Practice\SQL with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = GETDATE();
		print '>> crm_sales_details completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		print '=============================================';
		print 'Completed bulk loading into CRM tables';
		print '=============================================';
		print '=============================================';
		print 'Initializing ERP tables';
		print '=============================================';

		set @start_time = GETDATE();
		print '>> Truncating Table: Bronze.erp_CUST_AZ12';
		truncate table Bronze.erp_CUST_AZ12;

		print '>> Loading data into Bronze.erp_CUST_AZ12';
		bulk insert Bronze.erp_CUST_AZ12
		from 'C:\Users\yungr\OneDrive\Desktop\Professional Development\SQL Practice\SQL with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> erp_CUST_AZ12 completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		set @start_time = GETDATE();
		print '>> Truncating Table: Bronze.erp_LOC_A101';
		truncate table Bronze.erp_LOC_A101;

		print '>> Loading data into Bronze.erp_LOC_A101';
		bulk insert Bronze.erp_LOC_A101
		from 'C:\Users\yungr\OneDrive\Desktop\Professional Development\SQL Practice\SQL with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> erp_LOC_A101 completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'
	
		set @start_time = GETDATE();
		print '>> Truncating Table: Bronze.erp_PX_CAT_G1V2';
		truncate table Bronze.erp_PX_CAT_G1V2;

		print '>> Loading data into Bronze.erp_PX_CAT_G1V2';
		bulk insert Bronze.erp_PX_CAT_G1V2
		from 'C:\Users\yungr\OneDrive\Desktop\Professional Development\SQL Practice\SQL with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> erp_PX_CAT_G1V2 completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		print '=============================================';
		print 'Completed bulk loading into ERP tables';
		print '=============================================';

		set @end_batch = GETDATE();
		print '>> Bronze Layer Loading Complete';
		print '>> TOTAL LOAD DURATION: ' + cast(datediff(second, @start_batch, @end_batch) as nvarchar) + ' seconds';
	end try
	begin catch
		print '=============================================';
		print 'ERROR OCCURRED DURING LOADING OF BRONZE LAYER';
		print 'Error Message' + error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '=============================================';

	end catch
end
