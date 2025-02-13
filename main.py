from dbt.cli.main import dbtRunner



if __name__ == '__main__':
    
    dbt = dbtRunner()
    cli_args = ["build"]
    dbt.invoke(cli_args)