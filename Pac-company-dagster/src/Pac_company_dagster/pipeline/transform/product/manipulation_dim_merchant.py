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

def manipulation_dim_merchant(path_data_cleaned):
    df = spark.read.parquet(path_data_cleaned)
    dim_merchant = df.select(
        'product_merchant'
    ).distinct()

    # rename column 
    dim_merchant = dim_merchant.withColumnRenamed('product_merchant','merchant_name')

    dim_merchant = dim_merchant.withColumn("merchant_id", F.expr("uuid()"))

    output_path = os.path.join(DIR_STORED_FCT_DIM_PRODUCT, 'dim_merchant.parquet')
    dim_merchant.write.mode('overwrite').parquet(output_path)
    print(f" dim_merchant saved to {output_path}")
    spark.stop()
    return output_path