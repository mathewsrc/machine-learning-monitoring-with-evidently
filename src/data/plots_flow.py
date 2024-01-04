
import asyncio
from prefect import flow, task
from prefect_shell import shell_run_command

@flow
async def create_plots_flow():
    commands = [
       'poetry run python src/visualizations/visualizer.py contracts-by-contract-type',
       'poetry run python src/visualizations/visualizer.py contracts-by-department',
       'poetry run python src/visualizations/visualizer.py contracts-by-outstanding-orders',
       'poetry run python src/visualizations/visualizer.py contracts-by-profit-interesting',
       'poetry run python src/visualizations/visualizer.py contracts-by-purchasing-authority',
       'poetry run python src/visualizations/visualizer.py contracts-by-source-type', 
       'poetry run python src/visualizations/visualizer.py contracts-by-supplier-prime', 
       'poetry run python src/visualizations/visualizer.py contracts-over-time',
       'poetry run python src/visualizations/visualizer.py correlation-matrix',
    ]
    for command in commands:
        await shell_run_command(command=command, return_all=True)

if __name__ == "__main__":
    asyncio.run(create_plots_flow())