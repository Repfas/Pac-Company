from sqlalchemy import create_engine 
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv 
import os 

load_dotenv()

def dbsource_connection():
    try: 
        dwh_db_name = os.getenv('DWH_POSTGRES_DB')
        host_dwh = os.getenv('DWH_POSTGRES_HOST')
        user_dwh = os.getenv('DWH_POSTGRES_USER')
        pass_dwh = os.getenv('DWH_POSTGRES_PASSWORD')
        port_dwh = os.getenv('DWH_POSTGRES_PORT')
        dwh_conn = f'postgresql://{user_dwh}:{pass_dwh}@{host_dwh}:{port_dwh}/{dwh_db_name}'
        dwh_engine = create_engine(dwh_conn)
        return dwh_engine

    except Exception as e:
        print(e)
        
def dwh_connection():
    pass 