
  
    import pandas as pd
import io
import os
import numpy as np
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    pd.set_option('future.no_silent_downcasting', True)
    load_dotenv('.env', override=True)
    bucket_name = os.getenv('gcp_bucket')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='wellbeing')
    blob = next(blobs, None)
    data = blob.download_as_bytes()

    sheets = {
        '1 Life satisfaction means': 11,
        '4 Worthwhile means': 11,
        '7 Happiness means': 11,
        '10 Anxiety means': 12
    }

    sheet_frames = []

    for sheet, columns_row in sheets.items():

        df = pd.read_excel(io.BytesIO(data), sheet_name=sheet, engine='openpyxl')
        df.columns = df.iloc[columns_row,:]
        df = df.iloc[columns_row+1:,1:]

        df = df.replace('[cv1]', '<5%')
        df = df.replace('[cv2]', '5-10%')
        df = df.replace('[cv3]', '10-20%')
        df = df.replace('[cv4]', '>20%')
        df = df.replace('[u]', np.nan)
        df = df.replace('[x]', np.nan)

        index_column = 'Area Codes'

        melted_df = pd.DataFrame()
        i = 1
        while i < len(df.columns):
            value_column = df.columns[i]
            error_column = df.columns[i+1]
            temp_df = df[[index_column, value_column, error_column]].copy()
            temp_df['date'] = value_column
            temp_df.columns = ['area codes', 'value', 'margin of error', 'date']
            melted_df = pd.concat([melted_df, temp_df], ignore_index=True)
            i += 2

        melted_df['wellbeing_factor'] = sheet
        sheet_frames.append(melted_df)
    df = pd.concat(sheet_frames)

    df['wellbeing_factor'] = df['wellbeing_factor'].apply(lambda x: ' '.join(x.split(' ')[1:]))
    df['value'] = df['value'].astype(float)
    df = df.reset_index(names='ID')
    df.columns = [x.lower().replace(' ','_') for x in df.columns]

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
    identifier = "raw__wellbeing"
    
    def __repr__(self):
        return '"WMCSRP2"."md_raw"."raw__wellbeing"'


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
    tmp_name = '__dbt_python_model_df_' + 'raw__wellbeing__dbt_tmp'
    con.register(tmp_name, df)
    con.execute('create table "WMCSRP2"."md_raw"."raw__wellbeing__dbt_tmp" as select * from ' + tmp_name)
    con.unregister(tmp_name)

  