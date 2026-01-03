from dagster import asset_check,OpExecutionContext,AssetCheckResult, MetadataValue,TableRecord
from Pac_company_dagster.defs.asset.product import transform_product
from pyspark.sql import SparkSession
from pyspark.sql import functions as F




@asset_check(asset=transform_product)
def qc_cleaned_data_product(context: OpExecutionContext, clean_data_path): 
    '''This function is a quality check of cleaned_data_product'''
    spark = SparkSession.builder.getOrCreate()
    context.log.info('Data quality checking for cleaned data')
    df = spark.read.parquet(clean_data_path)
    # check the basic matrics. The number of row and columns
    context.log.info('Checking a number of rows and colums')
    row_count = df.count()
    column_count = len(df.columns)
    # check the null column 
    context.log.info('Checking a number of null columns')
    null_data = []
    total_nulls = 0 
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        total_nulls += null_count
        null_data.append(
            TableRecord(
                {
                    "column": col,
                    "null_count": null_count,
                }
            )
        )
    # check the duplicate
    context.log.info('Checking The duplicate data')
    duplicate_count = row_count - df.dropDuplicates().count()
    context.log.info('Checking The duplicate data')
    try:
        passed = row_count > 0 
    
    except Exception as e:
        context.log.info(f'Quality check is not passed {e}')

    return AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": row_count,
            "column_count": column_count,
            "data_shape": f"({row_count}, {column_count})",
            "duplicate_count": duplicate_count,
            "total_nulls": total_nulls,
            "nulls_per_column": MetadataValue.table(null_data),
        },
    )