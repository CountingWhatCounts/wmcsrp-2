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
    blobs = bucket.list_blobs(prefix='services')

    frames = []
    for blob in blobs:
        data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(data))
        df.columns = [x.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(':', '_') for x in df.columns]
        frames.append(df)
    df = pd.concat(frames)
    df = df.drop('amount_awarded', axis=1)
    df['postcode'] = df['postcode'].apply(lambda x: x.replace(' ', ''))

    return df