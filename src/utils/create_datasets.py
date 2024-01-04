import duckdb

def get_experiments_batches(end_date_0):
    with duckdb.connect("supplier.db") as conn:
        experiment_batches_sql = conn.sql(
            f"""
            WITH rank_month AS (
            SELECT
                start_date,
                EXTRACT(MONTH FROM start_date) AS month,
                RANK() OVER (ORDER BY month) AS rank
            FROM
                supplier_transformed_table
            WHERE start_date > '{end_date_0}' 
            )
            SELECT
                MIN(start_date),
                DATE_ADD(MIN(start_date), INTERVAL 6 MONTH) AS end_date,
            FROM
                rank_month
            GROUP BY
                rank
            LIMIT 10;
            """
        )
        experiment_batches=[(start.strftime("%Y-%m-%d %H:%M:%S"),
                            end.strftime("%Y-%m-%d %H:%M:%S")) for start, end in
                            experiment_batches_sql.fetchall() ]
        
    return experiment_batches

def get_reference_period():
    with duckdb.connect("supplier.db") as conn:
        start_date = conn.sql(
            """
            SELECT MIN(start_date)
            FROM supplier_transformed_table
            """
            ).fetchone()

        start_date_0 = start_date[0].strftime("%Y-%m-%d %H:%M:%S")

        end_date = conn.sql(
            """
            SELECT MAX(start_date) - INTERVAL 3 YEAR AS end_date
            FROM supplier_transformed_table
            """).fetchone()

        end_date_0 = end_date[0].strftime("%Y-%m-%d %H:%M:%S")
        
        return start_date_0, end_date_0
    
def create_reference_dataset():
    with duckdb.connect("supplier.db") as conn:
        start_date_0, end_date_0 = get_reference_period()
        print(f"Reference period: {start_date_0} to {end_date_0}")

        conn.sql(
                f"""
                CREATE OR REPLACE TABLE reference_table AS
                SELECT *
                FROM supplier_transformed_table
                WHERE  end_date <= '{end_date_0}';
                SELECT * FROM reference_table LIMIT 5;
                """
        ).show()
    
        
def create_production_datasets():
    with duckdb.connect("supplier.db") as conn:
        start_date_0, end_date_0 = get_reference_period()
        experiment_batches = get_experiments_batches(end_date_0)
        
        print(f"Experiment batches: {experiment_batches}")
        
        for i, (start_date, end_date) in enumerate(experiment_batches):
            conn.sql(
                f"""
                CREATE OR REPLACE TABLE production_table_{i} AS
                SELECT *
                FROM supplier_transformed_table
                WHERE start_date >= '{start_date}' AND  end_date <= '{end_date}'
                """
            )
    
        conn.sql("""
                DROP TABLE IF EXISTS production_table;
                 
                CREATE TABLE production_table AS
                SELECT *
                FROM production_table_0;
                """
                )
        
        for i in range(len(experiment_batches)):
            conn.sql(
                f"""
                INSERT INTO production_table
                SELECT * FROM production_table_{i} ;
                """
            )
            
        # print table data
        conn.sql(
            f"""
            SELECT *
            FROM production_table
            LIMIT 5
            """
        ).show()
            
if __name__ == "__main__":
    create_reference_dataset()
    create_production_datasets()