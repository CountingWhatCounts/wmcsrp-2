import pandas as pd
import io
import os
import numpy as np
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    bucket_name = os.getenv('WMCSRP_BUCKET')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='rural_urban_classification')
    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data))
    df.columns = [x.lower() for x in df.columns]

    return df