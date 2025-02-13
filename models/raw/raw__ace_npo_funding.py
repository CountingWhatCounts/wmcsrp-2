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
    blobs = bucket.list_blobs(prefix='ace_npo_funding')

    blob = next(blobs, None)
    data = blob.download_as_bytes()

    df = pd.read_excel(io.BytesIO(data), sheet_name='data', engine="openpyxl")
    df.columns = [x.lower().replace(' ','_').replace('\n', '_').replace('/','_').replace('(','_').replace(')','_') for x in df.columns]
    df = df.rename(columns={
        "2018-22_average_annual_funding__figure_accurate_at_april_2018_": "average_annual_funding_2018_22",
        "2022_23_annual_funding__extension_year_": "annual_funding__extension_year_2022_23",
        "2023-26_annual_funding__offered_4_nov_2022_": "annual_funding__offered_4_nov_2022_2023_26",
    })

    return df