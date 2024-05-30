-- Create database
create database if not exists amazon_sales_dwh;


-- Create Schemas
use database amazon_sales_dwh;

create schema if not exists source; -- will have source stage etc
create schema if not exists curated; -- data curation and de-duplication
create schema if not exists consumption; -- fact & dimension
create schema if not exists audit; -- to capture all audit records
create schema if not exists common; -- for file formats sequence object etc


-- Create Stage (Internal)
use schema source;
create or replace stage data_stg;


-- Grant permissions for all items to sysadmin (you can create and use different role too)
-- It is not necessary to provide "ALL ON ALL" permission, here I have used just for simplicity.

GRANT ALL ON ALL SCHEMAS IN DATABASE amazon_sales_dwh TO ROLE sysadmin;
GRANT ALL ON ALL TABLES IN SCHEMA amazon_sales_dwh.source TO ROLE sysadmin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA amazon_sales_dwh.source TO ROLE sysadmin;
GRANT ALL PRIVILEGES ON STAGE data_stg TO sysadmin;

-- List stage files
use schema source;
list @data_stg;
