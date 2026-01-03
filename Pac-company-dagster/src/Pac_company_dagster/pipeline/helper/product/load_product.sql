INSERT INTO dim_product
SELECT * 
FROM file('/data/dim_product.parquet', 'Parquet');

INSERT INTO dim_date 
SELECT * 
FROM file('/data/dim_date.parquet', 'Parquet');

INSERT INTO dim_merchant 
SELECT * 
FROM file('/data/dim_merchant.parquet', 'Parquet');

INSERT INTO dim_product_category 
SELECT * 
FROM file('/data/dim_product_category.parquet', 'Parquet');

INSERT INTO fct_product_price 
SELECT * 
FROM file('/data/fct_product_price.parquet', 'Parquet');