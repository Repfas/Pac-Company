import pandas as pd 
import os 
from dotenv import load_dotenv
import warnings
from Pac_company_dagster.pipeline.utils.conn import dbsource_connection

load_dotenv()
# define directory
DIR_ROOT_PROJECT =os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA_RAW = os.getenv("DIR_TEMP_DATA_RAW")
DIR_LOG = os.getenv("DIR_LOG")
DIR_SOURCE_DATA = os.getenv('DIR_SOURCE_DATA')
DIR_DATASET = os.getenv('DIR_SOURCE_DATA')

def extract_sales():
    conn = dbsource_connection()
    dest_path = os.path.join(DIR_TEMP_DATA_RAW, 'amazon_sales_data.csv')
    query = f'SELECT * FROM {'amazon_sales_data'};'
    df = pd.read_sql(query, conn)
    # save to csv
    df.to_csv(dest_path, index=False)   
    return dest_path


