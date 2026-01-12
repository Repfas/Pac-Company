# Data Engineering Pipeline

A complete batch ETL pipeline built for learning and hands-on practice in modern data engineering workflows.

---

## Overview

This project implements an **end-to-end batch data pipeline** using:

- Dagster – for orchestration  
- PySpark – for scalable data transformation  
- Parquet – as intermediate data storage  
- ClickHouse – as the analytical data warehouse

---

## How to Run

Follow the steps below to get the pipeline up and running:

### 1. Create temp folder 
mkdir Pac-company-dagster/src/Pac_company_dagster/pipeline/temp/data_raw
mkdir Pac-company-dagster/src/Pac_company_dagster/pipeline/temp/data_cleaned
mkdir Pac-company-dagster/src/Pac_company_dagster/pipeline/temp/fct_dim/product
mkdir Pac-company-dagster/src/Pac_company_dagster/pipeline/temp/fct_dim/sales

###2. Clone the Repository

git clone https://github.com/Repfas/Pac-Company  
cd Pac-company-dagster  
python -m venv venv  
source venv/bin/activate  
cd Pac-company-dagster

### 3. Run Source Database (PostgreSQL via Docker)

docker run -d \
  -p 5434:5432 \
  --name pac-company \
  shandytp/amazon-sales-data-docker-db:latest

### 4. Run ClickHouse (Analytical Warehouse)

docker run -d \
  -p 8123:8123 \
  -p 9000:9000 \
  -v /mnt/c/Users/UsEr/Documents/Belajar_Data_Engineer/final_project_wrangling_mod/Pac-company-dagster/src/Pac_company_dagster/pipeline/temp:/var/lib/clickhouse/user_files \
  -e CLICKHOUSE_USER=user \
  -e CLICKHOUSE_PASSWORD=password123 \
  --name clickhouse-server \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server

Validate the mount:

docker exec -it clickhouse-server ls /var/lib/clickhouse/user_files



###5. Create Databases and Tables (DDL)

docker exec -it clickhouse-server clickhouse-client

Then inside the ClickHouse client:

SHOW DATABASES;  
SHOW TABLES FROM product_dwh;  
SHOW TABLES FROM sales_dwh;

### 6. Install Requirements

pip install -r requirements.txt  
pip install -e .

### 7. Configure Environment Variables

Edit the `.env` file with your local configuration and paths.

### 8. Start Dagster UI

DAGSTER_GRPC_TIMEOUT_SECONDS=600 dagster dev

Then open: http://localhost:3000

### 9. Run the Pipeline

In the Dagster UI, materialize assets in order: Extract → Transform → Load

### 10. Validate Loaded Data

docker exec -it clickhouse-server clickhouse-client

Then run:

SELECT count(*) FROM product_dwh.dim_product;  
SELECT count(*) FROM product_dwh.dim_date;  
SELECT count(*) FROM product_dwh.fct_product_price;

---

## Tech Stack

- Python  
- Dagster  
- PySpark  
- Pandas  
- Parquet (PyArrow)  
- ClickHouse  
- Docker  

---

## Notes

- PySpark is used only in the Transform stage  
- Parquet is used as intermediate storage  
- ClickHouse loads are idempotent (TRUNCATE + INSERT)  
- Helper SQL files must be executed before running the pipeline

---

## Project Status

The ETL pipeline is now running end-to-end.

---

## Contributing

Feel free to fork the project and submit a pull request.

---

## License

This project is licensed under the MIT License.

---
