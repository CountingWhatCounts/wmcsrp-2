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
    blobs = bucket.list_blobs(prefix='ace_priority_places')

    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data))
    df.columns = [x.lower() for x in df.columns]

    return df