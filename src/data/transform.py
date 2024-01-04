import logging
from datetime import timedelta
from pyspark.sql import SparkSession
from prefect import task
import duckdb
from pyspark.sql.functions import (
    when, col, to_date)
from soda.check_function import check
from pathlib import Path

spark = SparkSession.builder.appName("SupplierTransform").getOrCreate()

def load_data(conn, table_name):
        df_pandas = conn.sql(f"SELECT * FROM {table_name}").df()
        df_spark = spark.createDataFrame(df_pandas)
        return df_spark

def rename_columns(df_spark):
    renamed_columns = {
        "contract_no": "contract_number",
        "consumed_amt": "purchase_orders_outstanding",
        "agreed_amt": "contract_awarded_amount",
        "pmt_amt": "payments_made",
        "remaining_amt": "remaining_contract_award_amount",
        "project_team_constituent": "supplier_type",
        "project_team_supplier": "supplier_name",
        "prime_contractor": "supplier_name_prime_contractor"
    }
    for old_col, new_col in renamed_columns.items():
        df_spark = df_spark.withColumnRenamed(old_col, new_col)
    return df_spark

def drop_duplicates_and_na(df_spark):
    df_spark = df_spark.dropDuplicates(subset=['contract_number'])
    df_spark = df_spark.na.drop(subset=["contract_number"])
    return df_spark

def cast_columns_to_double(df_spark):
    numeric_cols = ["purchase_orders_outstanding", "contract_awarded_amount", "payments_made", "remaining_contract_award_amount"]
    for col_name in numeric_cols:
        df_spark = df_spark.withColumn(col_name, col(col_name).cast("double"))
    return df_spark

def cast_columns_to_date(df_spark):
    df_spark = df_spark.withColumn("start_date", to_date('term_start_date')) 
    df_spark = df_spark.withColumn("end_date", to_date('term_end_date')) 
    return df_spark

def extract_year_month_day(df_spark):
    df_spark.createOrReplaceTempView("supplier_contracts_extract_year_month_day")
    df_spark = spark.sql("""
        SELECT
            *,
            EXTRACT(DAY FROM start_date) AS start_day,
            EXTRACT(MONTH FROM start_date) AS start_month,
            EXTRACT(YEAR FROM start_date) AS start_year,
            EXTRACT(DAY FROM end_date) AS end_day,
            EXTRACT(MONTH FROM end_date) AS end_month,
            EXTRACT(YEAR FROM end_date) AS end_year
        FROM supplier_contracts_extract_year_month_day
        WHERE start_date IS NOT NULL
                AND end_date IS NOT NULL
    """)
    return df_spark

def add_is_sole_source(df_spark):
    df_spark.createOrReplaceTempView("supplier_contracts_add_is_sole_source")
    df_spark = spark.sql("""
        SELECT *,
            CASE
                WHEN sole_source_flg = 'X' THEN 1
                ELSE 0
            END AS is_sole_source
        FROM supplier_contracts_add_is_sole_source
    """)
    return df_spark

def add_is_non_profit(df_spark):
    df_spark.createOrReplaceTempView("supplier_contracts_add_is_non_profit")
    df_spark = spark.sql("""
        SELECT *,
            CASE
                WHEN non_profit = 'X' THEN 1
            ELSE 0
            END AS is_non_profit
        FROM supplier_contracts_add_is_non_profit
    """)
    return df_spark

def add_has_outstanding_orders(df_spark):
    df_spark = df_spark.withColumn("has_outstanding_orders",
                                    when(col('purchase_orders_outstanding') > 0, 1).otherwise(0))
    return df_spark

def filter_positive_values(df_spark):
    numeric_columns = ["payments_made", "remaining_contract_award_amount",
                        "contract_awarded_amount", "purchase_orders_outstanding"]
    for col_name in numeric_columns:
        df_spark = df_spark.filter(col(col_name) >= 0)

    df_spark = df_spark.filter("payments_made <= contract_awarded_amount")
    df_spark = df_spark.filter("remaining_contract_award_amount <= contract_awarded_amount")
    df_spark = df_spark.filter((col("payments_made") + col("remaining_contract_award_amount")) <= col("contract_awarded_amount"))
    df_spark = df_spark.filter("purchase_orders_outstanding >= 0")

    return df_spark

@task(
    name="Transform Supplier Contract Data",
    description="Transform raw supplier contract data into a clean format",
    tags=['supplier', 'vendor', 'contract'],
    cache_expiration=timedelta(days=1),
    log_prints=True,
    retries=3,
    retry_delay_seconds=5
)
def transform():
    logging.info('Transforming Supplier Contract Data...')
    try:
        with duckdb.connect('supplier.db') as conn:
            df_spark = load_data(conn, 'supplier_raw_table')
            df_spark = rename_columns(df_spark)
            df_spark = drop_duplicates_and_na(df_spark)
            df_spark = cast_columns_to_double(df_spark)
            df_spark = cast_columns_to_date(df_spark)
            df_spark = extract_year_month_day(df_spark)
            df_spark = add_is_sole_source(df_spark)
            df_spark = add_is_non_profit(df_spark)
            df_spark = add_has_outstanding_orders(df_spark)
            df_spark = filter_positive_values(df_spark)

            #df_spark.write.mode('overwrite').parquet('supplier_transformed_table.parquet')
            df_spark.toPandas().to_parquet(Path('datasets/supplier_transformed_table.parquet'))
            
            conn.sql(f"""
                     CREATE OR REPLACE TABLE supplier_transformed_table AS
                     SELECT * FROM 'datasets/supplier_transformed_table.parquet';
                     DESCRIBE supplier_transformed_table;
                     """).show()
           
        return df_spark

    except Exception as e:
        logging.error(f"Error in transform function: {e}")
        raise e
    
    
@task(
    name="Check Staging Data",
    description="Run data quality checks on staging supplier contract data",
    tags=['supplier', 'vendor', 'contract'],
    cache_expiration=timedelta(days=1),
    retries=3,
    retry_delay_seconds=5
)
def check_staging():
    logging.info('Running data quality checks on staging data...')

    try:
        with duckdb.connect("supplier.db") as conn:
            check(scan_name="transformation_check", duckdb_conn=conn, data_source="duckdb", checks_subpath="staging")

    except Exception as e:
        logging.error(f"Error in check_staging function: {e}")
        raise e