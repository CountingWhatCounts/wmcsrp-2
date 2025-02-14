import pandas as pd
import io
import os
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    bucket_name = os.getenv('WMCSRP_BUCKET')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='indices_of_deprivation')
    
    for blob in blobs:
        if blob.name.endswith('.csv'):
            data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    df.columns = [x.lower().replace(' ', '_') for x in df.columns]

    return df