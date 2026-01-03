INSERT INTO dim_product
SELECT * 
FROM file('/data/sales/dim_product.parquet', 'Parquet');

INSERT INTO fct_sales_transaction 
SELECT * 
FROM file('/data/sales/fct_sales_transaction.parquet', 'Parquet');