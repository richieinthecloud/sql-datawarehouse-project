-- create a brand new database 



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