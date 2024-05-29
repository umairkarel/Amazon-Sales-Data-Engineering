-- Create warehouse
use role sysadmin;
create warehouse amazon_sales
    with
    warehouse_size = 'medium'
    warehouse_type = 'standard'
    auto_suspend = 60
    auto_resume = true
    min_cluster_count = 1
    max_cluster_count = 1;

-- Create User
use role accountadmin;
create user amazon_sales_user
    password = 'Test@123#'
    default_role = sysadmin
    default_secondary_roles = ('ALL')
    must_change_password = false;

-- Grant Roles
grant role sysadmin to user amazon_sales_user;
grant USAGE on warehouse amazon_sales to role sysadmin;
