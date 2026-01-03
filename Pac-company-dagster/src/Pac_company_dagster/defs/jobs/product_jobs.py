from dagster import define_asset_job



product_job = define_asset_job(
    name="product_job",
    selection=[
        "extract_product_raw",
        "transform_product",
        "create_dim_product",
        "create_dim_date",
        "create_dim_merchant",
        "create_dim_product_category",
        "create_fct_product_price",
        "load_dim_product_clickhouse",
        "load_dim_product_category_clickhouse",
        "load_dim_date_clickhouse",
        "load_dim_merchant_clickhouse",
        "load_fct_product_price_clickhouse",
    ],
)