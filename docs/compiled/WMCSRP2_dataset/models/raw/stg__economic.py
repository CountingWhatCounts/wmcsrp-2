import pandas as pd
import io
import numpy as np
import os
from google.cloud import storage
from dotenv import load_dotenv


def filter_columns(df: pd.DataFrame):
    column_indices = [0]
    i = 0
    while i < len(df.columns):
        if i + 3 < len(df.columns):
            column_indices.extend([i + 3, i + 4])
        i += 4

    df = df.iloc[:, column_indices]
    return df



def model(dbt, session):
    
    pd.set_option('future.no_silent_downcasting', True)
    load_dotenv('.env', override=True)
    bucket_name = os.getenv('gcp_bucket')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='economic')

    blob = next(blobs, None)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))

    df.columns = df.loc[5,:]
    df = df.loc[7:, :]
    df = filter_columns(df)

    new_columns = []
    new_columns.append(df.columns[0])
    i = 1
    while i < len(df.columns):
        if i + 1 < len(df.columns):
            new_columns.append(df.columns[i] + ' - Value')
            new_columns.append(df.columns[i] + ' - Margin of Error')
        i += 2
    df.columns = new_columns

    df = df.replace(['#', '-', '!', '*', '~'], np.nan)
    df = df.dropna(how='all')

    local_authority_col = 'local authority: district / unitary (as of April 2021)'

    # Identify value and margin columns
    value_columns = [col for col in df.columns if 'Value' in col]

    # Create a mapping from 'Value' columns to their corresponding 'Margin of Error' columns
    value_to_margin_map = {
        value_col: value_col.replace('Value', 'Margin of Error')
        for value_col in value_columns
    }

    # Prepare an empty DataFrame to store the melted data
    melted_df = pd.DataFrame()

    # Melt the DataFrame for each pair of value and margin columns
    for value_col, margin_col in value_to_margin_map.items():
        temp_df = df[[local_authority_col, value_col, margin_col]].copy()
        temp_df.columns = ['local authority', 'value', 'margin of error']
        temp_df['measure'] = value_col.replace(' - Value', '')
        melted_df = pd.concat([melted_df, temp_df], ignore_index=True)

    melted_df = melted_df.reset_index(names='ID')
    melted_df = melted_df[['ID', 'local authority', 'measure', 'value', 'margin of error']]
    melted_df['value'] = melted_df['value'].apply(lambda x: float(x) / 100)
    melted_df['margin of error'] = melted_df['margin of error'].apply(lambda x: float(x) / 100)
    melted_df.columns = [x.lower().replace(' ','_') for x in melted_df.columns]

    return melted_df


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
    schema = "main_raw"
    identifier = "stg__economic"
    
    def __repr__(self):
        return '"WMCSRP2"."main_raw"."stg__economic"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


