from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv
import warnings



load_dotenv()
warnings.filterwarnings("ignore")
DIR_DATA_CLEAN = os.getenv('DIR_TEMP_DATA_CLEANED')
DIR_STORED_FCT_DIM_PRODUCT = os.getenv('DIR_TEMP_DATA_FCT_DIM_PRODUCT')
spark = (
    SparkSession.builder
    .appName("MyApp")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
) 

def manipulation_fct_product(path_data_cleaned,
                             dim_product,
                             dim_date,
                             dim_merchant,
                             dim_product_category):
    df = spark.read.parquet(path_data_cleaned)
    dim_date = spark.read.parquet(dim_date)
    dim_product = spark.read.parquet(dim_product)
    dim_merchant = spark.read.parquet(dim_merchant)
    dim_product_category = spark.read.parquet(dim_product_category)

    # create a bridge table
    bridge_table = (
    df.join(
        dim_product,
        df["product_id"] == dim_product["nk_product_id"],
        "left"
    )
    .join(
        dim_date,
        F.date_trunc("day", df["seen_at"]) == dim_date["full_date"],
        "left"
    )
    .join(
        dim_merchant,
        df["product_merchant"] == dim_merchant["merchant_name"],
        "left"
    )
    .join(
        dim_product_category,
        (df["product_primary_categories"] == dim_product_category["product_primary_categories"]) &
        (df["product_categories"] == dim_product_category["product_categories"]),
        "left"
    )
)

    fct_product_price = bridge_table.select(
        'sk_product_id',
        'date_id',
        'seen_at',
        'merchant_id',
        'product_category_id',
        'price_max',
        'price_min',
        'updated_at',
        'created_at'
    )
    # create the product_price_id 
    fct_product_price = fct_product_price.withColumn("product_price_id", F.expr("uuid()"))
    output_path = os.path.join(DIR_STORED_FCT_DIM_PRODUCT, 'fct_product_price.parquet')
    fct_product_price.write.mode('overwrite').parquet(output_path)
    print(f" fct_product_price saved to {output_path}")
    spark.stop()
    return output_path
    
    
