
CREATE DATABASE IF NOT EXISTS product_dwh;

CREATE TABLE IF NOT EXISTS dim_product
(
    sk_product_id String,
    nk_product_id String,
    product_name String,
    product_image_url String,
    product_source String,
    product_upc String,
    product_asins String,
    product_key String,
    product_availability String,
    dateAdded DateTime DEFAULT now(),
    dateUpdated DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY sk_product_id;


CREATE TABLE IF NOT EXISTS dim_merchant
(
    merchant_id String,
    merchant_name String
)
ENGINE = MergeTree
ORDER BY merchant_id;


CREATE TABLE IF NOT EXISTS dim_date
(
    date_id Int32,
    full_date Date,
    day Int32,
    month Int32,
    year Int32
)
ENGINE = MergeTree
ORDER BY date_id;

CREATE TABLE IF NOT EXISTS dim_product_category
(
    product_category_id String,
    product_primary_category String,
    product_category String
)
ENGINE = MergeTree
ORDER BY product_category_id;


CREATE TABLE IF NOT EXISTS fct_product_price
(
    product_price_id String,
    sk_product_id String,
    date_id Int32,
    seen_at DateTime,
    merchant_id String,
    product_category_id String,
    price_min Int32,
    price_max Int32,
    dateAdded DateTime DEFAULT now(),
    dateUpdated DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY
(
    sk_product_id,
    date_id,
    merchant_id,
    product_category_id
);
