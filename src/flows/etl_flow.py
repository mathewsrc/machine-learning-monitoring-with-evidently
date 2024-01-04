from prefect import flow
from data.extract import extract, check_raw
from data.load import load
from data.transform import transform, check_staging

@flow(name="ETL Supplier Contract API", log_prints=True)
def etl():
  extract()
  check_raw()
  df_spark = transform()
  check_staging()
  load(df_spark)
  
  
if __name__ == "__main__":
  etl()