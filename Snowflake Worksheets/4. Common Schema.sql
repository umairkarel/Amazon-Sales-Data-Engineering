use database amazon_sales_dwh;
use schema common;

-- create file formats csv (India), json (France), Parquet (USA)
create or replace file format my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- json file format with strip outer array true
create or replace file format my_json_format
  type = json
  strip_outer_array = true
  compression = auto;

-- parquet file format
create or replace file format my_parquet_format
  type = parquet
  compression = snappy;



-- Grant permissions for all items to sysadmin (you can create and use different role too)
-- It is not necessary to provide "ALL ON ALL" permission, here I have used just for simplicity.

GRANT ALL PRIVILEGES ON FILE FORMAT AMAZON_SALES_DWH.COMMON.MY_CSV_FORMAT TO sysadmin;
GRANT ALL PRIVILEGES ON FILE FORMAT AMAZON_SALES_DWH.COMMON.MY_JSON_FORMAT TO sysadmin;
GRANT ALL PRIVILEGES ON FILE FORMAT AMAZON_SALES_DWH.COMMON.MY_PARQUET_FORMAT TO sysadmin;
