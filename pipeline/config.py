import os
from dotenv import load_dotenv

load_dotenv('.env', override=True)

RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
PREPROCESSED_DATA_DIR = os.getenv('PREPROCESSED_DATA_DIR')
DATA_BACKEND = os.getenv("DATA_BACKEND", "duckdb")
BUCKET_NAME = os.getenv("WMCSRP_BUCKET")
DUCKDB_PATH = "wmcsrp2.duckdb"
if DATA_BACKEND == 'postgres':
    POSTGRES_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}?sslmode=require"