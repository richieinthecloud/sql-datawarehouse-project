/*-
=============================================================================
Create Bronze Layer - Data Ingestion Schema Creation
=============================================================================
Purpose of this Script: 
  Creates staging tables in the Bronze layer for raw data ingestion from CRM and ERP source systems. This script establishes 
  the initial landing zone for customer, product, sales, and location data before transformation and loading into 
  Silver/Gold layers
TABLES CREATED:
- Bronze.crm_cust_info      (Customer information from CRM)
- Bronze.crm_prd_info       (Product information from CRM)
- Bronze.crm_sales_details  (Sales transaction details from CRM)
- Bronze.erp_CUST_AZ12      (Customer demographics from ERP)
- Bronze.erp_LOC_A101       (Customer location data from ERP)
- Bronze.erp_PX_CAT_G1V2    (Product category data from ERP)

BIG WARNING:
This script will DROP and recreate all Bronze layer tables if they already exist. All existing data in these tables 
will be permanently lost. Ensure you have backups or confirmations before executing in production environments.
*/

-- data ingestion into the bronze layer 

--drop table if exists Bronze.crm_cust_info; 
create table Bronze.crm_cust_info 
(
	cst_id int,
	cst_key int,
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(15),
	cst_gndr nvarchar(15),
	cst_create_date date 
)
go

-- drop table if exists Bronze.crm_prd_info; 
create table Bronze.crm_prd_info
(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(10),
	prd_start_dt datetime,
	prd_end_dt datetime
)
go

-- drop table if exists Bronze.crm_sales_details; 
create table Bronze.crm_sales_details
(
	sls_ord_num nvarchar(20),
	sls_prd_key nvarchar(20),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
)
go

-- drop table if exists Bronze.erp_CUST_AZ12; 
create table Bronze.erp_CUST_AZ12
(
	CID nvarchar(13),
	BDATE date,
	GEN nvarchar(6)
)
go

-- drop table if exists Bronze.erp_LOC_A101; 
create table Bronze.erp_LOC_A101
(
	CID nvarchar(11),
	CNTRY nvarchar(50)
)
go

-- drop table if exists Bronze.erp_PX_CAT_G1V2; 
create table Bronze.erp_PX_CAT_G1V2
(
	ID nvarchar(10),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(3)
)
go
