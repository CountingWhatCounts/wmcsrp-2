import pandas as pd
import io
import os
from google.cloud import storage
from dotenv import load_dotenv


def get_geography_from_filename(filename: str) -> str:
    return filename.split('.')[0].split('-')[2]


def process_age_data(age_data: pd.DataFrame) -> pd.DataFrame:
    column_groups = {
        'Age: Age 16-19': ['Age: Aged 16 to 19 years; measures: Value'],
        'Age: Age 20-24': ['Age: Aged 20 to 24 years; measures: Value'],
        'Age: Age 25-29': [
            'Age: Aged 25 years; measures: Value',
            'Age: Aged 26 years; measures: Value',
            'Age: Aged 27 years; measures: Value',
            'Age: Aged 28 years; measures: Value',
            'Age: Aged 29 years; measures: Value'
        ],
        'Age: Age 30-34': [
            'Age: Aged 30 years; measures: Value',
            'Age: Aged 31 years; measures: Value',
            'Age: Aged 32 years; measures: Value',
            'Age: Aged 33 years; measures: Value',
            'Age: Aged 34 years; measures: Value'
        ],
        'Age: Age 35-39': [
            'Age: Aged 35 years; measures: Value',
            'Age: Aged 36 years; measures: Value',
            'Age: Aged 37 years; measures: Value',
            'Age: Aged 38 years; measures: Value',
            'Age: Aged 39 years; measures: Value'
        ],
        'Age: Age 40-44': [
            'Age: Aged 40 years; measures: Value',
            'Age: Aged 41 years; measures: Value',
            'Age: Aged 42 years; measures: Value',
            'Age: Aged 43 years; measures: Value',
            'Age: Aged 44 years; measures: Value'
        ],
        'Age: Age 45-49': [
            'Age: Aged 45 years; measures: Value',
            'Age: Aged 46 years; measures: Value',
            'Age: Aged 47 years; measures: Value',
            'Age: Aged 48 years; measures: Value',
            'Age: Aged 49 years; measures: Value'
        ],
        'Age: Age 50-54': [
            'Age: Aged 50 years; measures: Value',
            'Age: Aged 51 years; measures: Value',
            'Age: Aged 52 years; measures: Value',
            'Age: Aged 53 years; measures: Value',
            'Age: Aged 54 years; measures: Value'
        ],
        'Age: Age 55-59': [
            'Age: Aged 55 years; measures: Value',
            'Age: Aged 56 years; measures: Value',
            'Age: Aged 57 years; measures: Value',
            'Age: Aged 58 years; measures: Value',
            'Age: Aged 59 years; measures: Value'
        ],
        'Age: Age 60-64': [
            'Age: Aged 60 years; measures: Value',
            'Age: Aged 61 years; measures: Value',
            'Age: Aged 62 years; measures: Value',
            'Age: Aged 63 years; measures: Value',
            'Age: Aged 64 years; measures: Value'
        ],
        'Age: Age 65-69': [
            'Age: Aged 65 years; measures: Value',
            'Age: Aged 66 years; measures: Value',
            'Age: Aged 67 years; measures: Value',
            'Age: Aged 68 years; measures: Value',
            'Age: Aged 69 years; measures: Value'
        ],
        'Age: Age 70-74': [
            'Age: Aged 70 years; measures: Value',
            'Age: Aged 71 years; measures: Value',
            'Age: Aged 72 years; measures: Value',
            'Age: Aged 73 years; measures: Value',
            'Age: Aged 74 years; measures: Value'
        ],
        'Age: Age 75-79': [
            'Age: Aged 75 years; measures: Value',
            'Age: Aged 76 years; measures: Value',
            'Age: Aged 77 years; measures: Value',
            'Age: Aged 78 years; measures: Value',
            'Age: Aged 79 years; measures: Value'
        ],
        'Age: Age 80-84': [
            'Age: Aged 80 years; measures: Value',
            'Age: Aged 81 years; measures: Value',
            'Age: Aged 82 years; measures: Value',
            'Age: Aged 83 years; measures: Value',
            'Age: Aged 84 years; measures: Value'
        ],
        'Age: Age 85 and over': ['Age: Aged 85 years and over; measures: Value']
        
    }

    df = age_data.copy()
    for new_col, existing_columns in column_groups.items():
        df[new_col] = df[existing_columns].sum(axis=1)

    final_columns = ['date','geography','geography code','Age: Total; measures: Value'] + list(column_groups.keys())
    df = df[final_columns]

    return df



def model(dbt, session):

    load_dotenv('.env', override=True)
    bucket_name = os.getenv('gcp_bucket')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='census')

    frames = []
    for blob in blobs:
        if not blob.name.endswith('.csv'):
            continue

        geography = get_geography_from_filename(blob.name)    
        if 'msoa' not in blob.name: # Only process MSOA data
            continue
            
        try:
            data = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(data))
        except pd.errors.EmptyDataError as e:
            continue

        # If age data then preprocess
        if 'ts007' in blob.name:
            df = process_age_data(df)

        total_column = [col for col in df.columns if 'Total' in col or 'All persons' in col][0] # Get total column
        data_content = total_column.split(':')[0] # Get data content from column
        value_columns = [col for col in df.columns if col not in ['date', 'geography', 'geography code', total_column]] # Get value columns

        df_totals = df[['date', 'geography', 'geography code', total_column]].drop_duplicates()
        df_values = df[['geography code'] + value_columns]
        df_values = df_values.melt(
            id_vars='geography code',
            var_name='measure',
            value_name='count'
        )

        df = df_totals.merge(df_values, how='left', on='geography code')
        df = df.rename(columns={total_column: 'n', 'date': 'year'})

        df['content'] = data_content
        df['geography'] = geography
        df = df[df['measure']!=total_column]
        df['measure'] = df['measure'].apply(lambda x: (':').join(x.split(':')[1:]))
        frames.append(df)

    df = pd.concat(frames)
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
    identifier = "raw__census"
    
    def __repr__(self):
        return '"WMCSRP2"."md_raw"."raw__census"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


