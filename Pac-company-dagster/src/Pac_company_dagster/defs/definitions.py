from dagster import Definitions, in_process_executor
# import from product
from Pac_company_dagster.defs.asset.product import *
from Pac_company_dagster.defs.jobs.product_jobs import product_job
from Pac_company_dagster.defs.schedules.product_schedules import product_daily_schedule
from Pac_company_dagster.defs.checks.product.check_data_product_cleaned import qc_cleaned_data_product
from Pac_company_dagster.defs.checks.product.check_dim_date import qc_dim_date
from Pac_company_dagster.defs.checks.product.check_raw_data_product import qc_raw_data_product
from Pac_company_dagster.defs.checks.product.check_fct_product import qc_fct_product_price
from Pac_company_dagster.defs.checks.product.check_dim_merchant import qc_dim_merchant
from Pac_company_dagster.defs.checks.product.check_dim_product import qc_dim_product
from Pac_company_dagster.defs.checks.product.check_dim_product_categoty import qc_dim_product_category
# import from sales
from Pac_company_dagster.defs.asset.sales import * 
from Pac_company_dagster.defs.jobs.sales_jobs import sales_job
from Pac_company_dagster.defs.schedules.sales_schedule import sales_daily_schedule
from Pac_company_dagster.defs.checks.sales.check_fct_sales import qc_fct_sales_price
from Pac_company_dagster.defs.checks.sales.check_data_sales_cleaned import qc_cleaned_data_sales
from Pac_company_dagster.defs.checks.sales.check_dim_product_sales import qc_dim_product_sales
from Pac_company_dagster.defs.checks.sales.check_raw_data_sales import qc_raw_data_sales 
defs = Definitions(
    assets=[
        extract_product_raw,
        transform_product,
        create_dim_product,
        create_dim_date,
        create_dim_merchant,
        create_dim_product_category,
        create_fct_product_price,
        extract_sales_raw,  
        transform_sales,
        create_dim_product_sales,
        create_fct_sales_price,
        load_dim_product_clickhouse,
        load_dim_date_clickhouse,
        load_dim_merchant_clickhouse,
        load_dim_product_category_clickhouse,
        load_fct_product_price_clickhouse,
        load_dim_product_sales_clickhouse,
        load_fct_sales_transaction_clickhouse
    ],
    jobs=[
        product_job,
        sales_job],
    schedules=[
        product_daily_schedule,
        sales_daily_schedule],
    executor=in_process_executor,
    asset_checks=[
        qc_cleaned_data_sales,
        qc_dim_product_sales,
        qc_raw_data_sales,
        qc_cleaned_data_product,
        qc_dim_product_category,
        qc_dim_date,
        qc_dim_merchant,
        qc_dim_product,
        qc_raw_data_product,
        qc_fct_sales_price
                  ]

)


