from dbt.cli.main import dbtRunner
from src.logger import logger

def run():
    logger.info("======== RUNNING DBT ========")
    dbt = dbtRunner()
    cli_args = ["run", "--full-refresh"]
    dbt.invoke(cli_args)
    logger.info("======== RUN COMPLETE ========\n\n")