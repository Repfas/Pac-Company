import pandas as pd 
import os 
from dotenv import load_dotenv
import warnings
from Pac_company_dagster.pipeline.utils.conn import dbsource_connection

load_dotenv()
DIR_ROOT_PROJECT =os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA_RAW = os.getenv("DIR_TEMP_DATA_RAW")
DIR_LOG = os.getenv("DIR_LOG")
DIR_SOURCE_DATA = os.getenv('DIR_SOURCE_DATA')


def extract_product():
    try:
        dest_path =  os.path.join(DIR_TEMP_DATA_RAW, 'electronic_products_data.csv')
        df = pd.read_csv(f'{DIR_SOURCE_DATA}/ElectronicsProductsPricingData.csv')
        df.to_csv(dest_path,index=False)
        return dest_path   
    except Exception as e:
        print(e)
        return pd.DataFrame
    
