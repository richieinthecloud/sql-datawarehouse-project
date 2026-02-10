/*
=============================================================================
Silver Layer - Transforming, Enriching and Cleaning Data
=============================================================================
Purpose of this Script: 
  Automated stored procedure to perform a wide array of transformation operations from Bronze layer (Loading & Staging) into the Silver layer. 
This procedure truncates existing data and loads data has been transformed utilizing what is in the Bronze layer. The cncludes error handling, load duration 
tracking, and detailed logging for monitoring ETL performance.

BIG WARNING:
  This procedure TRUNCATES all Silver layer tables before loading, again. 
Do not run this procedure unless you are certain the source Bronze layer tables are valid and accessible. Ensure all table query paths are correct
before execution. Running this in production without proper validation could result in data loss at the Silver layer. 
Always test in a development environment first.

Requirements: 
	Must have data from CSVs insert into the Bronze Layer tables
*/

create or alter procedure Silver.Load_Silver as
begin
	declare @start_time datetime, @end_time datetime, @start_batch datetime, @end_batch datetime;
	begin try 
		set @start_batch = GETDATE();
		print '=============================================';
		print 'Initializing Load (Bulk Insert) Procedure into Silver layer';
		print '=============================================';
		print '=============================================';
		print 'Initializing CRM tables';
		print '=============================================';

		set @start_time = GETDATE();
		print '>> Truncating table: Silver.crm_cust_info';
		truncate table silver.crm_cust_info;

		print '>> Loading data into Silver.crm_cust_info';
		insert into Silver.crm_cust_info 
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		select 
			cst_id,
			cst_key,
			trim(cst_firstname),
			trim(cst_lastname),
			case upper(trim(cst_marital_status))
				when 'M' then 'Married'
				when 'S' then 'Single'
				else 'Unrecorded'
			end cst_marital_status,
			case
				when cst_gndr = 'M' then 'Male'
				when cst_gndr = 'F' then 'Female'
				else 'Unrecorded'
			end cst_gndr,
			cst_create_date
		from (
			select *,
			ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			from Bronze.crm_cust_info
			where cst_id is not null
		) as t 
		where flag_last = 1;

		set @end_time = GETDATE();
		print '>> crm_cust_info completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'
	
		set @start_time = GETDATE();
		print '>> Truncating Table: Silver.crm_prd_info';
		truncate table silver.crm_prd_info;

		print '>> Loading data into Silver.crm_prd_info';
		insert into silver.crm_prd_info 
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
			prd_id,
			replace(substring(prd_key, 1, 5), '-', '_') as cat_id, -- derived column extracting category key
			substring(prd_key, 7, len(prd_key)) as prd_key, -- derived column extracting product key
			prd_nm,
			isnull(prd_cost, 0) as prd_cost, -- if the cost is null, replace it with a 0
			case Upper(Trim(prd_line)) 
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'M' then 'Mountain'
				when 'T' then 'Touring'
			else 'Unrecorded'
		end as prd_line, -- data enrichment. making it easier to understand 
		cast(prd_start_dt as date) as prd_start_dt, -- original table was datetime but we only really need date. time provides no value in this instance
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt -- calculate end date as one day before the next start date 
		from bronze.crm_prd_info

		set @end_time = GETDATE();
		print '>> crm_prd_info completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		set @start_time = GETDATE();
		print '>> Truncating Table: Silver.crm_sales_details';
		truncate table Silver.crm_sales_details;

		print '>> Loading data into Silver.crm_sales_details';
		insert into Silver.crm_sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt, 
			sls_sales,
			sls_quantity,
			sls_price
		)

		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case
				when sls_order_dt = 0 or len(sls_order_dt) !=8 then Null
				else cast(cast(sls_order_dt as varchar) as date) 
			end as sls_order_dt,
			case
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then Null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
			case 
				when sls_due_dt = 0 or len(sls_due_dt) != 8 then Null
				else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price) 
				else sls_sales
			end as sls_sales,
			sls_quantity,
			case 
				when sls_price is null or sls_price <= 0 
				then sls_sales / nullif(sls_quantity, 0)
				else sls_price
			end as sls_price
		from bronze.crm_sales_details
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
		print '>> Truncating Table: Silver.erp_CUST_AZ12';
		truncate table Silver.erp_CUST_AZ12;

		print '>> Loading data into Silver.erp_CUST_AZ12';
		insert into silver.erp_cust_az12
		(
			cid,
			cid_plus,
			BDATE,
			GEN
		)
		(
		select 
			cid,
			case 
				when cid like 'NAS%' then substring(cid, 4, len(cid))
				else cid 
			end as cid_plus,
			case 
				when bdate > getdate() then null
				else bdate
			end as bdate,
			case upper(trim(gen))
				when 'F' then 'Female'
				when 'Female' then 'Female'
				when 'M' then 'Male'
				when 'Male' then 'Male'
				else 'Unrecorded'
			end as gen
		-- upper(trim(gen)) in ('F', 'Female') then 'Female'
		-- upper(trim(gen)) in ('M', 'Male') then 'Male'
		-- else 'Unrecorded' 
		from bronze.erp_CUST_AZ12 ) 

		set @end_time = GETDATE();
		print '>> erp_CUST_AZ12 completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'


		set @start_time = GETDATE();
		print '>> Truncating Table: Silver.erp_LOC_A101';
		truncate table Silver.erp_LOC_A101;

		print '>> Loading data into Silver.erp_LOC_A101';
		insert into silver.erp_LOC_A101
		(
			CID,
			CNTRY
		)
		select 
			replace(CID, '-', '') as CID,
			case 
				when (trim(cntry)) in ('United States', 'US', 'USA') then 'United States of America'
				when (trim(cntry)) in ('DE', 'Germany') then 'Germany'
				when (trim(cntry)) in ('Australia', 'AU', 'AUS') then 'Australia'
				when (trim(cntry)) in ('Canada', 'CA', 'CAN', 'CAD') then 'Canada'
				when (trim(cntry)) = '' or CNTRY is null then 'Unrecorded'
				else (trim(cntry))
			end as country
		from bronze.erp_LOC_A101;

		set @end_time = GETDATE();
		print '>> erp_LOC_A101 completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		set @start_time = GETDATE();
		print '>> Truncating Table: Silver.erp_PX_CAT_G1V2';
		truncate table Silver.erp_PX_CAT_G1V2;

		print '>> Loading data into Silver.erp_PX_CAT_G1V2';
		insert into silver.erp_PX_CAT_G1V2
		(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		select
			ID,
			trim(CAT),
			SUBCAT,
			MAINTENANCE
		from bronze.erp_PX_CAT_G1V2

		set @end_time = GETDATE();
		print '>> erp_PX_CAT_G1V2 completed!';
		print '>> LOAD DURATION: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'

		print '=============================================';
		print 'Completed bulk loading into ERP tables';
		print '=============================================';


	end try
	begin catch
		print '=============================================';
		print 'ERROR OCCURRED DURING LOADING OF SILVER LAYER';
		print 'Error Message' + error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '=============================================';
	end catch
end

/*******************************************************************************************/
/*END OF SILVER LOAD */
/*******************************************************************************************/


exec Silver.Load_Silver;
