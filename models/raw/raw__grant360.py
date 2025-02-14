import pandas as pd
import io
import os
from google.cloud import storage
from dotenv import load_dotenv


def model(dbt, session):

    bucket_name = os.getenv('WMCSRP_BUCKET')
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='grant360')

    frames = []
    for blob in blobs:
        data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(data))
        df.columns = [x.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(':', '_') for x in df.columns]
        frames.append(df)
    df = pd.concat(frames)
    
    df['award_date'] = df['award_date'].astype(str)
    df['award_date'] = df['award_date'].apply(lambda x: x.replace('/', '-'))
    df['award_date'] = df['award_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))

    return df
