import clickhouse_connect
from dotenv import load_dotenv
import os

load_dotenv()


def load_dim_merchant(transformed_path):
    host = os.getenv('CH_HOST')
    port = int(os.getenv('CH_PORT'))
    username = os.getenv('CH_USER')
    password = os.getenv('CH_PASSWORD')
    dim_fct_path = os.getenv('DIR_TEMP_DATA_FCT_DIM')
    transformed_path = transformed_path.replace(dim_fct_path, '')
    transformed_path = transformed_path.lstrip('/')

    ch = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password
    )
    truncate_query = """TRUNCATE TABLE product_dwh.dim_merchant"""
    ch.command(truncate_query)
    query = f"""
    INSERT INTO product_dwh.dim_merchant
    (
        merchant_id,
        merchant_name
    )
    SELECT 
        merchant_id,
        merchant_name
    FROM file('{transformed_path}/*.parquet', 'Parquet')
    """

    
    ch.command(
        query,
        settings={
            "async_insert": 0,
            "wait_end_of_query": 1
        }
    )
    