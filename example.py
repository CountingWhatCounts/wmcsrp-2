import duckdb
import pandas as pd


if __name__ == "__main__":

    # Create a connection to the DuckDB database
    con = duckdb.connect("wmcsrp2.duckdb")
    con.execute("""SET schema 'public_marts';""")

    # List all tables in the public_marts schema
    tables = con.execute("show tables;")
    column_names = [desc[0] for desc in tables.description]
    print(pd.DataFrame(tables.fetchall(), columns=column_names))

    # Query all data from the local_authority_profiles table
    result = con.execute("SELECT * FROM public_marts.local_authority_profiles")
    column_names = [desc[0] for desc in result.description]
    print(pd.DataFrame(result.fetchall(), columns=column_names))

    # View participation rates for attending the theatre
    result = con.execute("""SELECT * FROM public_marts.modelled_participation_statistics where participation_domain = 'Theatre play, drama, musical, Pantomime, Ballet or Opera'""")
    column_names = [desc[0] for desc in result.description]
    print(pd.DataFrame(result.fetchall(), columns=column_names))

    # Close the database connection
    con.close()