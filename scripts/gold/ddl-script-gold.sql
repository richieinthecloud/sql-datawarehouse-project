/*
=============================================================================
Gold Layer - Create Views for Business Analytics
=============================================================================
Purpose of this Script: 
  This script will create Views for the Gold layer of our medallion architecture for this data warehouse. The Gold layer 
  is the final transformations of the data to make it available for analysts within the company. Dims and Facts utilize the Star Schema

BIG WARNING:
  This procedure DROPS all Gold layer tables (Views) before loading, again. 
Do not run this procedure unless you are certain the source Silver layer tables are valid and accessible. Ensure all table query paths are correct
before execution. Running this in production without proper validation could result in data loss at the Gold layer. 
Always test in a development environment first.

Requirements: 
	Silver layer must have already been built out and sanitized. 
*/

		print('=============================================================================');
		print('Create Dimension: gold.dim_customers');
		print('=============================================================================');

		print('>>Dropping Table: Customer Dim');
		IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
			DROP VIEW gold.dim_customers
		GO

		create view gold.dim_customers as 
		 select 
			ROW_NUMBER() over (order by cst_id) as customer_key, -- This is the surrogate key
			ci.cst_id as customer_id, 
			ci.cst_key as customer_number,
			ci.cst_firstname as first_name,
			ci.cst_lastname as last_name,
			la.CNTRY as country,
			case 
				when ci.cst_gndr != 'Unrecorded' then ci.cst_gndr -- we are doing this because CRM is the master table in all these joins
				else coalesce(ca.gen, 'Unrecorded') 
			end as gender,
			ca.bdate as birth_date,
			ci.cst_marital_status as martital_status,
			ci.cst_create_date as create_date,
			ci.dwh_createdate
		from silver.crm_cust_info as ci
		left join silver.erp_CUST_AZ12 as ca
			on ci.cst_key = ca.CID_PLUS
		left join silver.erp_LOC_A101 as la
			on ci.cst_key = la.CID
		GO

		print('>> Gold.dim_customers completed!');

		-- I confirmed there are no duplicates after joining 3 tables 

		print('=============================================================================');
		print('Create Dimension: gold.dim_products');
		print('=============================================================================');

		IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
			DROP VIEW gold.dim_products;
		GO

		create view gold.dim_products as
		select 
			ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key, -- Surrogate key
			pn.prd_id as product_id,
			pn.prd_key as product_number,
			pn.prd_nm as product_name, 
			pn.cat_id as category_id, 
			px.CAT as category,
			px.SUBCAT as subcategory,
			px.MAINTENANCE as maintenance,
			pn.prd_cost as cost, 
			pn.prd_line as product_line,
			pn.prd_start_dt as start_date
		from silver.crm_prd_info as pn
		left join silver.erp_PX_CAT_G1V2 as px
			on pn.cat_id = px.ID
		where prd_end_dt is null
		GO

		print('>> Gold.dim_products completed!');

		-- we use this 'where' clause to make sure we are only indexing CURRENT products. No historization needed here.
		-- confirmed that there are no duplicate prd_key

		print('=============================================================================');
		print('Create Fact Table: gold.fact_sales');
		print('=============================================================================');

		IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
			DROP VIEW gold.fact_sales;
		GO

		create view gold.fact_sales as
		select 
			ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key, -- Surrogate key
			pn.prd_id as product_id,
			pn.prd_key as product_number,
			pn.prd_nm as product_name, 
			pn.cat_id as category_id, 
			px.CAT as category,
			px.SUBCAT as subcategory,
			px.MAINTENANCE as maintenance,
			pn.prd_cost as cost, 
			pn.prd_line as product_line,
			pn.prd_start_dt as start_date
		from silver.crm_prd_info as pn
		left join silver.erp_PX_CAT_G1V2 as px
			on pn.cat_id = px.ID
		where prd_end_dt is null
		GO

		print('>> Gold.facts_sales completed!');
