from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv
import warnings

load_dotenv()
warnings.filterwarnings("ignore")
DIR_DATA_CLEAN = os.getenv('DIR_TEMP_DATA_CLEANED')
DIR_TEMP_DATA_FCT_DIM_SALES = os.getenv('DIR_TEMP_DATA_FCT_DIM_SALES')
spark = (
    SparkSession.builder
    .appName("MyApp")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
) 

def manipulation_dim_product_sales(clean_path):
    df = spark.read.parquet(clean_path)
    # get the following col for dim_product. dont forget to distinct it 
    dim_product = df.select(
        'product_name',
        'product_main_category',
        'product_sub_category',
        'product_image_url',
        'product_url'
    ).distinct()
    # create a uuid on product_id for sugorrate key 
    dim_product = dim_product.withColumn("product_id", F.expr("uuid()"))

    output_path = os.path.join(DIR_TEMP_DATA_FCT_DIM_SALES, 'dim_product_sales.parquet')
    dim_product.write.mode('overwrite').parquet(output_path)
    print(f" dim_table saved to {output_path}")
    spark.stop()
    return output_path