import pandas as pd
import os
import yaml
from tempfile import TemporaryDirectory
import chardet


def get_table_name_from_csv(csv_name):
    return csv_name.split("__")[1].split(".")[0]


def filter_columns(df: pd.DataFrame):
    column_indices = [0]
    i = 0
    while i < len(df.columns):
        if i + 3 < len(df.columns):
            column_indices.extend([i + 3, i + 4])
        i += 4

    df = df.iloc[:, column_indices]
    return df


def estimate_row_size(df: pd.DataFrame, sample_size: int = 1000) -> int:
    with TemporaryDirectory() as temp_dir:
        sample_size = min(sample_size, len(df))
        sample_df = df.iloc[:sample_size]
        temp_sample_file = os.path.join(temp_dir, "temp_sample.csv")
        sample_df.to_csv(temp_sample_file, index=False)
        avg_row_size = os.path.getsize(temp_sample_file) / sample_size

    return avg_row_size


def save_half_df(
    df: pd.DataFrame,
    estimated_rows_per_chunk: int,
    df_row_count: int,
    start_row: int,
    temp_filename: str,
) -> None:
    estimated_rows_per_chunk = max(1, estimated_rows_per_chunk // 2)
    end_row = min(start_row + estimated_rows_per_chunk, df_row_count)
    temp_df = df.iloc[start_row:end_row]
    temp_df.to_csv(temp_filename, index=False)
    return end_row


def save_df_in_chunks(
    data_source: str, output_dir: str, df: pd.DataFrame, max_file_size_mb: int = 250
) -> None:
    MAX_FILE_SIZE = max_file_size_mb * 1024 * 1024
    avg_row_size = estimate_row_size(df)
    estimated_rows_per_chunk = int(
        MAX_FILE_SIZE / avg_row_size
    )  # Calculate the number of rows that approximately fit in the target file size
    df_row_count = len(df)

    csv_output_count = 1
    start_row = 0

    while start_row < df_row_count:
        end_row = min(start_row + estimated_rows_per_chunk, df_row_count)
        temp_df = df.iloc[start_row:end_row]

        temp_filename = os.path.join(
            output_dir, f"{data_source}_part_{csv_output_count}.csv"
        )
        temp_df.to_csv(temp_filename, index=False)
        file_size = os.path.getsize(temp_filename)

        # Check if the created file meets the size limit
        while file_size >= MAX_FILE_SIZE and estimated_rows_per_chunk > 1:
            # File is too large, reduce the number of rows and write again
            end_row = save_half_df(
                df, estimated_rows_per_chunk, df_row_count, start_row, temp_filename
            )
            file_size = os.path.getsize(temp_filename)

        print(f"Saved {temp_filename} with size {file_size / (1024 * 1024):.2f} MB")
        start_row = end_row
        csv_output_count += 1


def get_data_folders(directory: str) -> list[str]:
    return [f for f in os.listdir(directory) if f != ".DS_Store"]


def process_age_data(age_data: pd.DataFrame) -> pd.DataFrame:
    column_groups = {
        "Age: Age 16-19": ["Age: Aged 16 to 19 years; measures: Value"],
        "Age: Age 20-24": ["Age: Aged 20 to 24 years; measures: Value"],
        "Age: Age 25-29": [
            "Age: Aged 25 years; measures: Value",
            "Age: Aged 26 years; measures: Value",
            "Age: Aged 27 years; measures: Value",
            "Age: Aged 28 years; measures: Value",
            "Age: Aged 29 years; measures: Value",
        ],
        "Age: Age 30-34": [
            "Age: Aged 30 years; measures: Value",
            "Age: Aged 31 years; measures: Value",
            "Age: Aged 32 years; measures: Value",
            "Age: Aged 33 years; measures: Value",
            "Age: Aged 34 years; measures: Value",
        ],
        "Age: Age 35-39": [
            "Age: Aged 35 years; measures: Value",
            "Age: Aged 36 years; measures: Value",
            "Age: Aged 37 years; measures: Value",
            "Age: Aged 38 years; measures: Value",
            "Age: Aged 39 years; measures: Value",
        ],
        "Age: Age 40-44": [
            "Age: Aged 40 years; measures: Value",
            "Age: Aged 41 years; measures: Value",
            "Age: Aged 42 years; measures: Value",
            "Age: Aged 43 years; measures: Value",
            "Age: Aged 44 years; measures: Value",
        ],
        "Age: Age 45-49": [
            "Age: Aged 45 years; measures: Value",
            "Age: Aged 46 years; measures: Value",
            "Age: Aged 47 years; measures: Value",
            "Age: Aged 48 years; measures: Value",
            "Age: Aged 49 years; measures: Value",
        ],
        "Age: Age 50-54": [
            "Age: Aged 50 years; measures: Value",
            "Age: Aged 51 years; measures: Value",
            "Age: Aged 52 years; measures: Value",
            "Age: Aged 53 years; measures: Value",
            "Age: Aged 54 years; measures: Value",
        ],
        "Age: Age 55-59": [
            "Age: Aged 55 years; measures: Value",
            "Age: Aged 56 years; measures: Value",
            "Age: Aged 57 years; measures: Value",
            "Age: Aged 58 years; measures: Value",
            "Age: Aged 59 years; measures: Value",
        ],
        "Age: Age 60-64": [
            "Age: Aged 60 years; measures: Value",
            "Age: Aged 61 years; measures: Value",
            "Age: Aged 62 years; measures: Value",
            "Age: Aged 63 years; measures: Value",
            "Age: Aged 64 years; measures: Value",
        ],
        "Age: Age 65-69": [
            "Age: Aged 65 years; measures: Value",
            "Age: Aged 66 years; measures: Value",
            "Age: Aged 67 years; measures: Value",
            "Age: Aged 68 years; measures: Value",
            "Age: Aged 69 years; measures: Value",
        ],
        "Age: Age 70-74": [
            "Age: Aged 70 years; measures: Value",
            "Age: Aged 71 years; measures: Value",
            "Age: Aged 72 years; measures: Value",
            "Age: Aged 73 years; measures: Value",
            "Age: Aged 74 years; measures: Value",
        ],
        "Age: Age 75-79": [
            "Age: Aged 75 years; measures: Value",
            "Age: Aged 76 years; measures: Value",
            "Age: Aged 77 years; measures: Value",
            "Age: Aged 78 years; measures: Value",
            "Age: Aged 79 years; measures: Value",
        ],
        "Age: Age 80-84": [
            "Age: Aged 80 years; measures: Value",
            "Age: Aged 81 years; measures: Value",
            "Age: Aged 82 years; measures: Value",
            "Age: Aged 83 years; measures: Value",
            "Age: Aged 84 years; measures: Value",
        ],
        "Age: Age 85 and over": ["Age: Aged 85 years and over; measures: Value"],
    }

    df = age_data.copy()
    for new_col, existing_columns in column_groups.items():
        df[new_col] = df[existing_columns].sum(axis=1)

    final_columns = [
        "date",
        "geography",
        "geography code",
        "Age: Total; measures: Value",
    ] + list(column_groups.keys())
    df = df[final_columns]

    return df


def get_geography_from_filename(filename: str) -> str:
    return filename.split(".")[0].split("-")[2]


def get_total_column_name(df):
    return [col for col in df.columns if "Total" in col or "All persons" in col][0]


def get_data_content_from_total_column(total_column) -> str:
    return total_column.split(":")[0]


def get_value_columns(df: pd.DataFrame, total_column: str) -> list:
    return [
        col
        for col in df.columns
        if col not in ["date", "geography", "geography code", total_column]
    ]


def remove_content_from_measure_name(measure_name: str) -> str:
    return (":").join(measure_name.split(":")[1:])


def replace_invalid_characters(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [column_name.replace(" ", "_") for column_name in df.columns]
    df.columns = [column_name.replace("-", "_") for column_name in df.columns]
    df.columns = [column_name.replace("/", "") for column_name in df.columns]
    df.columns = [column_name.replace("(", "") for column_name in df.columns]
    df.columns = [column_name.replace(")", "") for column_name in df.columns]
    df.columns = [column_name.replace("?", "") for column_name in df.columns]
    df.columns = [column_name.replace(".", "") for column_name in df.columns]
    df.columns = [column_name.replace(",", "") for column_name in df.columns]
    df.columns = [column_name.replace("@", "") for column_name in df.columns]
    df.columns = [column_name.replace("+", "and_over") for column_name in df.columns]
    return df


def label_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = []
    counts = {}

    for col in df.columns:
        if col in counts:
            counts[col] += 1
            columns.append(f"{col}_{counts[col]}")
        else:
            counts[col] = 1
            columns.append(col)
    df.columns = columns
    return df


def append_underscore_if_number(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = ["_" + col if col[0].isdigit() else col for col in df.columns]
    return df


def get_config(file: str):
    with open(file) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config


def detect_encoding(file_path, sample_size=100000):
    with open(file_path, "rb") as f:
        raw_data = f.read(sample_size)
    return chardet.detect(raw_data)["encoding"]
