/*-
=============================================================================
Create Database and Schemas
=============================================================================
Purpose of this Script: 
  This will create the database we will be working out of, 'Data_Warehouse'. This script will check if the database exists and then drop it if it does. Once it has been dropped, it will be recreate from scratch. 
  Additionally, this script will create three schemas named after each stage in the medallion architecture. Bronze, Silver and Gold, accordingly. 

BIG WARNING:
    Running this script will drop the entire "Data_Warehouse" database, assuming you already have one built out. 
All the data in your database will be permanently delete with no means of recovery. Please be careful when you decide to execute this.

*/
-- Drops and recreates the 'Data_Warehouse' database 
if exists (Select 1 from sys.databases where name = 'Data_Warehouse')
  Begin
      alter database Data_Warehouse set single_user with rollback immediate; 
      drop database Data_Warehouse;
  End;
go

-- Create the database
create database Data_Warehouse;
go

use Data_Warehouse;
go

create schema Bronze;
go
create schema Silver;
go
create schema Gold;
go
