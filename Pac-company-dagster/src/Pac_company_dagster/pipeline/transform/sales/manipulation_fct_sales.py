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

def manipulation_fct_sales(path_data_cleaned,dim_product):
    df = spark.read.parquet(path_data_cleaned)
    dim_product = spark.read.parquet(dim_product)

     # create the fct_table
    # create the join_df 
    join_df = df.join(
        dim_product,
        on = [
            'product_name',
            'product_main_category',
            'product_sub_category',
            'product_image_url',
            'product_url'
        ],
        how= 'left'
    )
    #  select product_id on 
    fct_sales_transaction = join_df.select(
        'product_id',
        'product_rating',
        'product_rating_count',
        'product_price',
        'product_price_discount'
    )
    # create the sales_transaction_id for PK 
    fct_sales_transaction = fct_sales_transaction.withColumn("sales_transaction_id", F.expr("uuid()"))
    output_path = os.path.join(DIR_TEMP_DATA_FCT_DIM_SALES, 'fct_sales_transaction.parquet')
    fct_sales_transaction.write.mode('overwrite').parquet(output_path)
    print(f" fct_table saved to {output_path}")
    spark.stop()
    return output_path