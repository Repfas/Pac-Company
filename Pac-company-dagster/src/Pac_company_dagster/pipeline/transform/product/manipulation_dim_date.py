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

def generate_dim_date():
    start_date = '2013-01-01'
    end_date = '2020-12-31'
    dim_date = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
                 )
             ) AS full_date
        """)
        )

    dim_date = (dim_date
                .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
                .withColumn("day", F.dayofmonth("full_date").cast("int"))
                .withColumn("month", F.month("full_date").cast("int"))
                .withColumn("year", F.year("full_date").cast("int"))
    )
    output_path = os.path.join(DIR_STORED_FCT_DIM_PRODUCT, 'dim_date.parquet')
    dim_date.write.mode('overwrite').parquet(output_path)
    print(f" dim_date saved to {output_path}")
    spark.stop()
    return output_path

