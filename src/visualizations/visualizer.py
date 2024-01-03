import duckdb
from matplotlib import pyplot as plt
from pathlib import Path
import click
import seaborn as sns
import polars as pl
from datetime import timedelta

@click.group()
def cli():
    pass

@cli.command("contracts-by-contract-type")
@click.option("--limit", default=10, help="Number of contracts to show")
@click.option("--output-path", default="output/contracts-by-contract-type.png", help="Path to save the output image")
def contracts_by_contract_type(limit=10, output_path="output/contracts-by-contract-type.png"):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                contract_type,
                COUNT(contract_type) AS total
            FROM supplier_transformed_table
            WHERE contract_type IS NOT NULL
            GROUP BY contract_type
            ORDER BY total DESC
            LIMIT 10
            """
            ).pl().sort("total")


        plt.barh(df_pl['contract_type'],
                df_pl['total'], color='skyblue')
        plt.title("Top 10 Contracts by Contract Type")
        plt.xlabel("Total")
        plt.show()
        
        if Path(output_path).parent.exists():
            plt.savefig(output_path)
            click.echo(click.style(f"Saved output to {output_path}", fg="green"))
        
        
@cli.command("contracts-by-purchasing-authority")
@click.option("--limit", default=10, help="Number of contracts to show")
@click.option("--output-path", default="output/contracts-by-purchasing-authority.png", help="Path to save the output image")
def contracts_by_purchasing_authority(limit, output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                purchasing_authority,
                COUNT(purchasing_authority) AS total
            FROM supplier_transformed_table
            WHERE purchasing_authority IS NOT NULL
            GROUP BY purchasing_authority
            ORDER BY total DESC
            LIMIT 10
            """
            ).pl().sort("total")


        plt.barh(df_pl['purchasing_authority'],
                df_pl['total'], color='skyblue')
        plt.title("Top 10 Contracts by Purchasing Authority")
        plt.xlabel("Total")
        plt.show()
        
        if Path(output_path).parent.exists():
            plt.savefig(output_path)
            click.echo(click.style(f"Saved output to {output_path}", fg="green"))
        
    
@cli.command("contracts-by-department")
@click.option("--limit", default=10, help="Number of contracts to show")
@click.option("--output-path", default="output/contracts-by-department.png", help="Path to save the output image")
def contracts_by_department(limit, output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                department,
                COUNT(department) AS total
            FROM supplier_transformed_table
            WHERE department IS NOT NULL
            GROUP BY department
            ORDER BY total DESC
            LIMIT 10
            """
            ).pl().sort("total")


        plt.barh(df_pl['department'],
                df_pl['total'], color='skyblue')
        plt.title("Top 10 Contracts by Department")
        plt.xlabel("Total")
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))   
        

@cli.command("contracts-by-supplier-prime")
@click.option("--limit", default=10, help="Number of contracts to show")
@click.option("--output-path", default="output/contracts-by-supplier-prime.png", help="Path to save the output image")
def contracts_by_supplier_prime(limit, output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                supplier_name_prime_contractor,
                COUNT(supplier_name_prime_contractor) AS total
            FROM supplier_transformed_table
            WHERE supplier_name_prime_contractor IS NOT NULL
            GROUP BY supplier_name_prime_contractor
            ORDER BY total DESC
            LIMIT 10
            """
            ).pl().sort("total")


        plt.barh(df_pl['supplier_name_prime_contractor'],
                df_pl['total'], color='skyblue')
        plt.title("Top 10 Contracts by Supplier Prime Contractor")
        plt.xlabel("Total")
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))


@cli.command("contracts-over-time")
@click.option("--output-path", default="output/contracts-over-time.png", help="Path to save the output image")
def contracts_over_time(output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                COUNT(contract_number) AS total,
                start_date
            FROM supplier_transformed_table
            GROUP BY start_date
            """
            ).pl().sort("start_date")


        plt.plot(df_pl["start_date"], df_pl["total"], marker='o', linestyle='-',
                color='skyblue', label='Total Contracts')

        plt.title('Number of Contracts Over Time')
        plt.xlabel('Year')
        plt.ylabel('Number of Contracts')
        plt.legend()
        plt.gcf().autofmt_xdate()
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))
        
@cli.command("contracts-by-source-type")
@click.option("--output-path", default="output/contracts-by-source-type.png", help="Path to save the output image")
def contracts_by_source_type(output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                CASE
                WHEN is_sole_source = 1 THEN 'Sole Source'
                ELSE 'Not Sole Source'
                END AS is_sole_source,
                COUNT(is_sole_source) AS total
            FROM supplier_transformed_table
            WHERE is_sole_source IS NOT NULL
            GROUP BY is_sole_source
            ORDER BY total DESC
            """
            ).pl().sort("total")

        plt.bar(df_pl['is_sole_source'],
                df_pl['total'], color='skyblue')
        plt.title("Number of Contracts by Source type")
        plt.xlabel("Total")
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))


@cli.command("contracts-by-profit-interesting")
@click.option("--output-path", default="output/contracts-by-profit-interesting.png", help="Path to save the output image")
def contract_by_profit_interesting(output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                CASE
                WHEN is_non_profit = 1 THEN 'Non Profit'
                ELSE 'Profit'
                END AS is_non_profit,
                COUNT(is_non_profit) AS total
            FROM supplier_transformed_table
            WHERE is_non_profit IS NOT NULL
            GROUP BY
                is_non_profit
            ORDER BY total DESC
            """
            ).pl().sort("total")

        plt.bar(df_pl['is_non_profit'],
                df_pl['total'], color='skyblue')
        plt.title("Number of Contracts by Profit interesting")
        plt.xlabel("Total")
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))
        
@cli.command("contracts-by-outstanding-orders")
@click.option("--output-path", default="output/contracts-by-outstanding-orders.png", help="Path to save the output image")
def contracts_by_outstading_orders(output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pl = conn.sql(
            """
            SELECT
                CASE
                WHEN has_outstanding_orders = 1 THEN 'Has Outstanding Orders'
                ELSE 'Not has Outsanding Orders'
                END AS has_outstanding_orders,
                COUNT(has_outstanding_orders) AS total
            FROM supplier_transformed_table
            WHERE has_outstanding_orders IS NOT NULL
            GROUP BY
                has_outstanding_orders
            ORDER BY total DESC
            """
            ).pl().sort("total")

        plt.bar(df_pl['has_outstanding_orders'],
                df_pl['total'], color='skyblue')
        plt.title("Number of Contracts by Outstanding Orders")
        plt.xlabel("Total")
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))
        
@cli.command("correlation-matrix")
@click.option("--output-path", default="output/correlation-matrix.png", help="Path to save the output image")
def correlation_matrix(output_path):
    with duckdb.connect("supplier.db") as conn:
        df_pandas = conn.sql("SELECT * FROM supplier_transformed_table").df()
        corr_matrix = df_pandas.corr(numeric_only=True)
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f")
        plt.title('Correlation Matrix')
        plt.show()
        
    if Path(output_path).parent.exists():
        plt.savefig(output_path)
        click.echo(click.style(f"Saved output to {output_path}", fg="green"))

if __name__ == "__main__":
    if not Path("supplier.db").exists():
        raise Exception("Database not found. Please run the extract task first.")
    
    if not Path("output").exists():
        Path("output").mkdir()
    
    cli()
    