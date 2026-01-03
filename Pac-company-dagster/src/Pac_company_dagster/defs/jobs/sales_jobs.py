from dagster import define_asset_job



sales_job = define_asset_job(
    name="sales_job",
    selection=[
        "extract_sales_raw",
        'transform_sales',
        'create_dim_product_sales',
        'create_fct_sales_price',
        'load_dim_product_sales_clickhouse',
        'load_fct_sales_transaction_clickhouse'
    ],
)