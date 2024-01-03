from prefect import flow
from extract import extract, check_raw
from load import load
from transform import transform, check_staging

@flow(name="ETL Supplier Contract API", log_prints=True)
def etl():
  extract()
  check_raw()
  df_spark = transform()
  check_staging()
  load(df_spark)
  
  
if __name__ == "__main__":
  etl()