/*-
=============================================================================
Create Silver Layer - Data Cleaning Stage
=============================================================================
Purpose of this Script: 
  Creates an intermediate layer in the Silver layer where we have already transformed that data from the CRM and ERP source systems. This script establishes 
  a zone where the data is better prepared to be used by business analyst. The data will be usable in the Gold layer 

TABLES CREATED:
- Silver.crm_cust_info      (Customer information from CRM)
- Silver.crm_prd_info       (Product information from CRM)
- Silver.crm_sales_details  (Sales transaction details from CRM)
- Silver.erp_CUST_AZ12      (Customer demographics from ERP)
- Silver.erp_LOC_A101       (Customer location data from ERP)
- Silve.erp_PX_CAT_G1V2    (Product category data from ERP)

BIG WARNING:
This script will DROP and recreate all Bronze layer tables if they already exist. All existing data in these tables 
will be permanently lost. Ensure you have backups or confirmations before executing in production environments.
*/

-- Clean data in the Silver Layer 

drop table if exists Silver.crm_cust_info; 
create table Silver.crm_cust_info 
(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(15),
	cst_gndr nvarchar(15),
	cst_create_date date,
	dwh_createdate datetime2 default getdate()
)
go

drop table if exists Silver.crm_prd_info; 
create table Silver.crm_prd_info
(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(15),
	prd_start_dt date,
	prd_end_dt date,
	dwh_createdate datetime2 default getdate()
)
go

drop table if exists Silver.crm_sales_details; 
create table Silver.crm_sales_details
(
	sls_ord_num nvarchar(20),
	sls_prd_key nvarchar(20),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_createdate datetime2 default getdate()
)
go

drop table if exists Silver.erp_CUST_AZ12; 
create table Silver.erp_CUST_AZ12
(
	CID nvarchar(50),
	CID_PLUS nvarchar(50),
	BDATE date,
	GEN nvarchar(50),
	dwh_createdate datetime2 default getdate()
)
go

drop table if exists Silver.erp_LOC_A101; 
create table Silver.erp_LOC_A101
(
	CID nvarchar(11),
	CNTRY nvarchar(50),
	dwh_createdate datetime2 default getdate()
)
go

drop table if exists Silver.erp_PX_CAT_G1V2; 
create table Silver.erp_PX_CAT_G1V2
(
	ID nvarchar(10),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(3),
	dwh_createdate datetime2 default getdate()
)
go
