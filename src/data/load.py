import logging
from datetime import timedelta
from prefect import task
import duckdb


@task(
    name="Load Transformed Data",
    description="Load transformed supplier contract data into DuckDB",
    tags=['supplier', 'vendor', 'contract'],
    cache_expiration=timedelta(days=1),
    retries=3,
    retry_delay_seconds=5
)
def load(df_spark, table_name="supplier_transformed_table"):
    logging.info(f'Loading transformed data into DuckDB table: {table_name}')

    try:
        with duckdb.connect("supplier.db") as conn:
            df_pandas = df_spark.toPandas()
            conn.sql(f"""CREATE OR REPLACE TABLE {table_name} AS
                         SELECT * FROM df_pandas""")

    except Exception as e:
        logging.error(f"Error in load function: {e}")
        raise e