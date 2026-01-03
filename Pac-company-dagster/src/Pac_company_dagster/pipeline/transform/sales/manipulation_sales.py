from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as ps
import os
from tabulate import tabulate
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore")


load_dotenv()
spark = (
    SparkSession.builder
    .appName("MyApp")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
) 

DIR_DATA_RAW = os.getenv('DIR_TEMP_DATA_RAW')
DIR_STORED_DATA = os.getenv('DIR_TEMP_DATA_CLEANED')

def sales_manipulation(extract_path):
    # read_data
    df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", True) \
    .option("mode", "PERMISSIVE") \
    .csv(os.path.join(extract_path))

    # change the column name
    new_col_name = {
        'name':'product_name',
        "main_category": "product_main_category",
        "sub_category": "product_sub_category",
        "image": "product_image_url", 
        'link': "product_url",
        'ratings': "product_rating",
        'no_of_ratings': "product_rating_count",
        'discount_price': "product_price_discount", 
        'actual_price': "product_price",
    }
    df = df.withColumnsRenamed(colsMap = new_col_name)

    # drop the unname_column 
    df = df.drop('Unnamed: 0')

    # clean the product_price. delete the ₹ and keep the number only
    # delete the ₹
    df = df.withColumn(
        'product_price',
            F.regexp_replace(F.col('product_price'),'₹',''))
        # delet the comma 
    df = df.withColumn(
        'product_price',
        F.regexp_replace(F.col('product_price'),',','') 
        )
    
    df = df.withColumn(
        'product_price',F.col('product_price').cast('double').cast('int')
    )
    # drop NULL value
    df = df.na.drop(subset=['product_price'])

   # clean the product_price_discount. delete the ₹ and keep the number only
    # delete the ₹
    df = df.withColumn(
        'product_price_discount',
            F.regexp_replace(F.col('product_price_discount'),'₹',''))
        # delet the comma 
    df = df.withColumn(
        'product_price_discount',
        F.regexp_replace(F.col('product_price_discount'),',','') 
        )
    # cast type to int
    df = df.withColumn(
        'product_price_discount',F.col('product_price_discount').cast('double').cast('int')
    )
    # clean the product_rating_count
    # delete usually..., only...,Free...,
    df = df.withColumn('product_rating_count',
                       F.when(F.lower(F.col('product_rating_count')).rlike('usually|only|free'), '0')
                        .otherwise(F.col('product_rating_count') ))
    # delete the comma product_rating count
    df = df.withColumn(
        'product_rating_count',
        F.regexp_replace(F.col('product_rating_count'),',','') 
        )
    # cast type to int
    df = df.withColumn(
        'product_rating_count',F.col('product_rating_count').cast('int')
    )
    # fill null value 
    df = df.fillna({'product_rating_count': 0})


    # clean the product_rating
    # delete usually..., only...,Free...,
    df = df.withColumn('product_rating',
                       F.when(F.lower(F.col('product_rating')).rlike('free|₹|get'), '0')
                        .otherwise(F.col('product_rating') ))

    # cast type to int
    df = df.withColumn(
        'product_rating',F.col('product_rating').cast('float')
    )
    # fill null value 
    df = df.fillna({'product_rating': 0})
    

    # create new col of 10% product_price 
    df = df.withColumn('price_discount(2)',(F.round(F.col('product_price')*0.1))) 
    df = df.withColumn('imputed',F.when(F.col("product_price_discount").isNull(),F.col('price_discount(2)')).otherwise(F.col("product_price_discount"))) 
    # drop price_discount and discount(2) 
    df = df.drop("product_price_discount").drop('price_discount(2)') 
    # rename imputed col 
    df = df.withColumnRenamed('imputed',"product_price_discount")
    # drop duplicate 
    df = df.dropDuplicates()
    # add created_at 
    df = df.withColumn("created_at", F.current_timestamp())


    
    # export to csv 
    output_path = os.path.join(DIR_STORED_DATA, 'sales_clean.parquet')
    df.write.mode('overwrite').parquet(output_path)
    print(f" Cleaned sales data saved to {output_path}")
    spark.stop()
    return output_path
