
  
    import pandas as pd
import io
import os
import numpy as np
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    load_dotenv('.env', override=True)
    bucket_name = os.getenv('gcp_bucket')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='postcode_mapping')
    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data))
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df.columns = [x.lower() for x in df.columns]
    df['pcd_no_space'] = df['pcd2'].apply(lambda x: x.replace(' ', ''))

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
    identifier = "raw__postcode_mapping"
    
    def __repr__(self):
        return '"WMCSRP2"."md_raw"."raw__postcode_mapping"'


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
    tmp_name = '__dbt_python_model_df_' + 'raw__postcode_mapping__dbt_tmp'
    con.register(tmp_name, df)
    con.execute('create table "WMCSRP2"."md_raw"."raw__postcode_mapping__dbt_tmp" as select * from ' + tmp_name)
    con.unregister(tmp_name)

  