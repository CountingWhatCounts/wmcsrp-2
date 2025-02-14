import pandas as pd
import io
import os
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    bucket_name = os.getenv('WMCSRP_BUCKET')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='ace_project_grants')

    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_excel(io.BytesIO(data), sheet_name='Project Grants Awards', engine="openpyxl")
    df.columns = df.iloc[1, :]
    df = df.iloc[2:, :]
    df.columns = [x.lower().replace(' ','_') for x in df.columns]

    return df