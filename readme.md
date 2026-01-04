Data Engineering Pipeline
Dagster · PySpark · ClickHouse

Overview
This project implements a batch data engineering pipeline for learning and practice. It demonstrates an end-to-end ETL workflow using:

Dagster for orchestration

PySpark for data transformation

Parquet as intermediate storage

ClickHouse as the analytical data warehouse

How to Run
1. Clone the repository
bash
git clone <REPO_URL>
cd Pac-company-dagster
2. Run source database (PostgreSQL)
bash
docker run -d \
  -p 5434:5432 \
  --name pac-company \
  shandytp/amazon-sales-data-docker-db:latest
3. Run ClickHouse (Data Warehouse)
bash
docker run -d \
  -p 8123:8123 \
  -p 9000:9000 \
  -v /mnt/c/Users/UsEr/Documents/Belajar_Data_Engineer/final_project_wrangling_mod/Pac-company-dagster/src/Pac_company_dagster/pipeline/temp:/var/lib/clickhouse/user_files \
  -e CLICKHOUSE_USER=user \
  -e CLICKHOUSE_PASSWORD=password123 \
  --name clickhouse-server \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server
Validate mount:

bash
docker exec -it clickhouse-server ls /var/lib/clickhouse/user_files
4. Create databases and tables (DDL)
All schemas are defined in helper SQL files.

bash
cat product/helper_product.sql | docker exec -i clickhouse-server clickhouse-client
cat sales/helper_sales.sql     | docker exec -i clickhouse-server clickhouse-client
Verify:

bash
docker exec -it clickhouse-server clickhouse-client
Then in the ClickHouse client:

sql
SHOW DATABASES;
SHOW TABLES FROM product_dwh;
SHOW TABLES FROM sales_dwh;
5. Set up Python environment
bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
6. Configure environment variables
Create a .env file in the project root:

env
CH_HOST=localhost
CH_PORT=8123
CH_USER=user
CH_PASSWORD=password123
DIR_TEMP_DATA_FCT_DIM=/mnt/c/Users/UsEr/Documents/Belajar_Data_Engineer/final_project_wrangling_mod/Pac-company-dagster/src/Pac_company_dagster/pipeline/temp
7. Start Dagster
bash
DAGSTER_GRPC_TIMEOUT_SECONDS=600 dagster dev
Open: http://localhost:3000

8. Run the pipeline
In Dagster UI, materialize assets from Extract → Transform → Load.

9. Validate loaded data
bash
docker exec -it clickhouse-server clickhouse-client
Then in the ClickHouse client:

sql
SELECT count(*) FROM product_dwh.dim_product;
SELECT count(*) FROM product_dwh.dim_date;
SELECT count(*) FROM product_dwh.fct_product_price;


Tech Stack
Python

Dagster

PySpark

Pandas

Parquet (PyArrow)

ClickHouse

Docker

Notes
PySpark is used only in the Transform stage

Parquet is used as intermediate storage

ClickHouse loads are idempotent (TRUNCATE + INSERT)

Helper SQL files must be executed before running the pipeline

Status
The ETL pipeline is now running end-to-end.
