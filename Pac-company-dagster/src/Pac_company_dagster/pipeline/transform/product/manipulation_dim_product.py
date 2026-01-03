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

def manipulation_dim_product(path_data_cleaned):
    df = spark.read.parquet(path_data_cleaned)
    # CREATE DIM PRODUCT 
    dim_product = df.select(
        'product_id',
        'product_name',
        'product_image_url',
        'product_source',
        'upc',
        'asins',
        'product_key',
        'product_availability'
    ).distinct()
    # change the column name 
    new_name = {
        'product_id':'nk_product_id',
        'upc':'product_upc',
        'asins':'product_asins'
    }
    dim_product = dim_product.withColumnsRenamed(new_name)
    # create the dim product_id 
    dim_product = dim_product.withColumn("sk_product_id", F.expr("uuid()"))

    output_path = os.path.join(DIR_STORED_FCT_DIM_PRODUCT, 'dim_product.parquet')
    dim_product.write.mode('overwrite').parquet(output_path)
    print(f" dim_product saved to {output_path}")
    spark.stop()
    return output_path

