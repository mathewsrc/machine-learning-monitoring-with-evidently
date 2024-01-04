import os
import logging
import requests
import pandas as pd
from datetime import timedelta
from prefect import task
from pathlib import Path
import duckdb
from soda.check_function import check

@task(
    name="Extract Supplier Contract API data",
    description="Get API data from URL and read it as Spark DataFrame",
    tags=['supplier', 'vendor', 'contract'],
    cache_expiration=timedelta(days=1),
    retries=0,
    retry_delay_seconds=5
)
def extract():

    logging.info('Extracting Supplier Contract API data...')

    if not os.path.exists("supplier.db"):
        logging.info("Database not found; downloading data from API.")

        try:
            api_url = 'https://data.sfgov.org/resource/cqi5-hm2d.json?$limit=1000'
            response = requests.get(api_url)
            response.raise_for_status()  

            data = response.json()
            df_api = pd.json_normalize(data)
            
            if not Path('datasets').exists():
                Path('datasets').mkdir(exist_ok=True)
                
            df_api.to_parquet(Path('datasets/supplier_raw_table.parquet'))
            
            with duckdb.connect("supplier.db") as conn:
                conn.sql("""
                         CREATE OR REPLACE TABLE supplier_raw_table AS
                         SELECT * FROM 'datasets/supplier_raw_table.parquet';
                         DESCRIBE supplier_raw_table
                         """).show()
                
        except requests.RequestException as e:
            raise Exception(f'Error extracting data from API: {e}')

    else:
        logging.info("Database found; API not called.")
        with duckdb.connect("supplier.db") as conn:
            conn.sql("SELECT COUNT(*) FROM supplier_raw_table").show()

@task(
    name="Check Raw Supplier Contract Data",
    description="Run data quality checks on raw supplier contract data",
    tags=['supplier', 'vendor', 'contract'],
    cache_expiration=timedelta(days=1),
    retries=3,
    retry_delay_seconds=5
)
def check_raw():
    logging.info('Running data quality checks on raw supplier contract data...')

    with duckdb.connect("supplier.db") as conn:
        check(scan_name="raw_check", duckdb_conn=conn, data_source="duckdb", checks_subpath="sources")