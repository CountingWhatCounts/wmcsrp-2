
  
    import pandas as pd
import io
import os
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    load_dotenv('.env', override=True)
    bucket_name = os.getenv('gcp_bucket')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='ace_npo_funding')

    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_excel(io.BytesIO(data), sheet_name='data', engine="openpyxl")
    df.columns = [x.lower().replace(' ','_').replace('\n', '_').replace('/','_').replace('(','_').replace(')','_') for x in df.columns]
    df = df.rename(columns={
        "2018-22_average_annual_funding__figure_accurate_at_april_2018_": "average_annual_funding_2018_22",
        "2022_23_annual_funding__extension_year_": "annual_funding__extension_year_2022_23",
        "2023-26_annual_funding__offered_4_nov_2022_": "annual_funding__offered_4_nov_2022_2023_26",
    })

    return df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "WMCSRP2"
    schema = "md_raw"
    identifier = "raw__ace_npo_funding"
    
    def __repr__(self):
        return '"WMCSRP2"."md_raw"."raw__ace_npo_funding"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------




def materialize(df, con):
    try:
        import pyarrow
        pyarrow_available = True
    except ImportError:
        pyarrow_available = False
    finally:
        if pyarrow_available and isinstance(df, pyarrow.Table):
            # https://github.com/duckdb/duckdb/issues/6584
            import pyarrow.dataset
    tmp_name = '__dbt_python_model_df_' + 'raw__ace_npo_funding__dbt_tmp'
    con.register(tmp_name, df)
    con.execute('create table "WMCSRP2"."md_raw"."raw__ace_npo_funding__dbt_tmp" as select * from ' + tmp_name)
    con.unregister(tmp_name)

  