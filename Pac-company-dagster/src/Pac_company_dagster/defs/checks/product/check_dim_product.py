from dagster import asset_check,OpExecutionContext,AssetCheckResult, MetadataValue,TableRecord
from Pac_company_dagster.defs.asset.product import create_dim_product
from pyspark.sql import SparkSession
from pyspark.sql import functions as F




@asset_check(asset=create_dim_product)
def qc_dim_product(context: OpExecutionContext, dim_product_path): 
    '''This function is a quality check of dim_product'''
    spark = SparkSession.builder.getOrCreate()
    context.log.info('Data quality checking for dim_product')
    df = spark.read.parquet(dim_product_path)
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

    # Primary key check 
    context.log.info('Checking the primary key')
    pk_column = "sk_product_id"
    pk_null_count = (
        df.filter(F.col(pk_column).isNull()).count()
        if pk_column in df.columns
        else row_count
    )
    context.log.info('Checking the Data Quality ')
    try:
        passed = (row_count > 0) and (pk_null_count == 0)
        context.log.info('Quality Check: PASSED')
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
            "pk_null_count": pk_null_count,
            "nulls_per_column": MetadataValue.table(null_data),
        },
    )