import duckdb

def main():
    with duckdb.connect("supplier.db") as conn:
        conn.sql("""
            SELECT *
            FROM supplier_transformed_table
            LIMIT 5
            """
        ).show()
        
if __name__ == "__main__":
    main()
    