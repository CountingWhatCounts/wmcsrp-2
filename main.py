import os
from dbt.cli.main import dbtRunner
import duckdb
import pandas as pd
from sqlalchemy import create_engine



if __name__ == '__main__':
    
    # dbt = dbtRunner()
    # cli_args = ["build"]
    # dbt.invoke(cli_args)


    local_conn = duckdb.connect("WMCSRP2.duckdb")
    schema = 'md_marts'

    # supabase_url = f"postgresql://postgres:StyNhQJjvQSxFbRS@db.xoelzzgpxyiwjnjcozqm.supabase.co:5432/postgres"
    # remote_conn = create_engine(dbname='postgres', user='postgres.xoelzzgpxyiwjnjcozqm', password='StyNhQJjvQSxFbRS', host='aws-0-eu-west-2.pooler.supabase.com', port='5432')
    remote_conn = create_engine(f"postgresql://postgres.xoelzzgpxyiwjnjcozqm:{os.getenv('SUPABASE_PASSWORD')}@aws-0-eu-west-2.pooler.supabase.com:6543/postgres")
    print(os.getenv('SUPABASE_PASSWORD'))
    tables = local_conn.sql("SHOW ALL TABLES").fetchdf()
    tables = list(tables[tables['schema'] == schema]['name'])
    print(tables)

    for table in tables:
        print(f"Transferring table: {table}...")
        df = local_conn.execute(f"SELECT * FROM {schema}.{table}").fetchdf()
        df.to_sql(table, remote_conn, if_exists="replace", index=False)
        print(f"✅ Table {table} successfully transferred.")