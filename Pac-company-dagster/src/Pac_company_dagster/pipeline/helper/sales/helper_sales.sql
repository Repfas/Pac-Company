CREATE DATABASE IF NOT EXISTS sales_dwh;
CREATE TABLE IF NOT EXISTS dim_product ( 
	product_id String, 
	product_name  String, 
	product_sub_category String, 
	product_image_url String, 
	prodcut_link String 
        )
    ENGINE = MergeTree
    ORDER BY product_id;
        
CREATE TABLE IF NOT EXISTS fct_sales_transaction( 
	sales_transaction_id UUID, 
	product_id UUID, 
	product_rating Float32,
	product_rating_count Int32, 
	product_price_discount Float32, 
	product_price Int32 )

    ENGINE = MergeTree
    ORDER BY product_id;