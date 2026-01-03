from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as ps
import os
from tabulate import tabulate
from dotenv import load_dotenv
import warnings
from Pac_company_dagster.pipeline.extract.extract_product import extract_product
warnings.filterwarnings("ignore")


load_dotenv()
spark = (
        SparkSession.builder
        .appName("product_manipulation")
        .getOrCreate()
    )

DIR_DATA_RAW = os.getenv('DIR_TEMP_DATA_RAW')
DIR_STORED_DATA = os.getenv('DIR_TEMP_DATA_CLEANED')

def product_manipulation(raw_product_path):
    # read_data
    df = (
    spark.read
    .option("header", True)
    .option("sep", ",")        
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .option("mode", "PERMISSIVE")
    .csv(raw_product_path)
)
    # drop table unnamed 26-30, ean, shipping,
    drop_col = ['Unnamed: 26','Unnamed: 27',"Unnamed: 28",'Unnamed: 29','Unnamed: 30','ean','shipping',
                'isSale','currency','sourceURLs.1']
    for col in drop_col:
        df = df.drop(col)
    # change new name 
    column_mapping = {
    "id": "product_id",
    "amountMax": "price_max",
    "amountMin": "price_min",
    "availability": "product_availability",
    "condition": "product_condition",
    "dateSeen": "seen_at",
    "merchant": "product_merchant",
    "sourceURLs": "product_source",
    'categories':'product_categories',
    "brand": "product_brand",
    "dateAdded": "created_at",
    "dateUpdated": "updated_at",
    "imageURLs": "product_image_url",
    "keys": "product_key",
    "manufacturer": "product_manufacturer",
    "manufacturerNumber": "product_manufacturer_id",
    "name": "product_name",
    "primaryCategories": "product_primary_categories",
    "upc": "upc",
    "weight": "product_weight",
}
    df = df.withColumnsRenamed(column_mapping)
    # clean product_availability. the product is only available or not_available
    df = df.withColumn('product_availability',
                       F.when(F.col('product_availability').rlike('TRUE'), 'available')
                       .when(F.col('product_availability').rlike('in_stock'),'available')
                       .when(F.col('product_availability').rlike('More on the Way'),'available')
                       .when(F.lower(F.col('product_availability')).rlike('yes'), 'available')
                       .when(F.col('product_availability').rlike('Special Order'),'available')
                       .when(F.col('product_availability').rlike('available'),'available')
                       .otherwise('not available'))
    # clean product_condition. the product condition should be a new, used and refubrished
    df = df.withColumn(
        "product_condition",
        F.when((F.col("product_condition").rlike("new")), "new")
        .when((F.col("product_condition").rlike("used|pre-owned")), "used")
        .when((F.col("product_condition").rlike("refurbished")), "refurbished")
        .otherwise(F.col("product_condition")))
    
    # cleaned data seen_at. get the first date of seen_at
    df = df.withColumn(
        'seen_at',
        F.expr("array_min(split(seen_at, ','))")
    )

    # cleaned asins. get the first asins only 
    df = df.withColumn(
        'asins',
        F.expr("array_min(split(asins, ','))")
    )
    # fill na product_manufacturer 
    df = df.fillna({'product_manufacturer': 'unknown'})


    # cast typedata 
    # float
    float_type_col = ['price_max','price_min']
    for col in float_type_col:
        df = df.withColumn(col, F.col(col).cast('float'))
    # datetime 
    datetime_type_col = ['seen_at','created_at','updated_at']
    for col in datetime_type_col:
        df = df.withColumn(col,F.to_timestamp(F.trim(F.col(col))))
    # drop datetime_type null value 

        df = df.na.drop(subset=[col])

    # export to csv 
    output_path = os.path.join(DIR_STORED_DATA, 'product_clean.parquet')
    df.write.mode('overwrite').parquet(output_path)
    print(f" Cleaned sales data saved to {output_path}")
    spark.stop()
    return output_path


