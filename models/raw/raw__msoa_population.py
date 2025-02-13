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
    blobs = bucket.list_blobs(prefix='msoa_population')
    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_excel(io.BytesIO(data), sheet_name='Mid-2021 MSOA 2021', engine='openpyxl')
    df.columns = df.iloc[2, :]
    df = df.iloc[3:, :]
    df = df[['MSOA 2021 Code', 'Total']]
    df.columns = [x.lower().replace(' ', '_') for x in df.columns]
    df = df.rename(columns={'msoa_2021_code': 'msoa21cd', 'total': 'population'})

    return df