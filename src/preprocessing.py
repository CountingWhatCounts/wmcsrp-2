import pandas as pd
import numpy as np
import os
import pyreadstat

from .logger import logger
from .helper_functions import (
    save_df_in_chunks,
    get_data_folders,
    get_data_content_from_total_column,
    get_geography_from_filename,
    get_total_column_name,
    get_value_columns,
    process_age_data,
    remove_content_from_measure_name,
    filter_columns,
    replace_invalid_characters,
    label_duplicate_columns,
    append_underscore_if_number
)


def ace_project_grants(
        downloaded_data_dir: str,
        preprocessed_data_dir: str
    ) -> None:

    logger.info(f"Pre-processing ACE Project Grants data")
    data_dir = os.path.join(downloaded_data_dir, 'ace_project_grants')
    files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name='Project Grants Awards')
    df.columns = df.iloc[1, :]
    df = df.iloc[2:, :]
    df.columns = [x.lower().replace(' ','_') for x in df.columns]

    output_path = os.path.join(preprocessed_data_dir, f"ace_project_grants.csv")
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def ace_npo_funding(
        downloaded_data_dir: str,
        preprocessed_data_dir: str
    ) -> None:

    logger.info(f"Pre-processing ACE NPO Funding data")
    data_dir = os.path.join(downloaded_data_dir, 'ace_npo_funding')
    files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name='data')
    df.columns = [x.lower().replace(' ','_').replace('\n', '_').replace('/','_').replace('(','_').replace(')','_') for x in df.columns]

    output_path = os.path.join(preprocessed_data_dir, 'npo_funding_data.csv')
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")




def economic_data(
        downloaded_data_dir: str,
        preprocessed_data_dir: str
    ) -> None:

    logger.info(f"Pre-processing Annual Survey Economic data")
    data_dir = os.path.join(downloaded_data_dir, 'economic')
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    pd.set_option('future.no_silent_downcasting', True)

    df = pd.read_csv(os.path.join(data_dir, files[0]))
    df.columns = df.loc[5,:]
    df = df.loc[7:, :]

    df = filter_columns(df)

    new_columns = []
    new_columns.append(df.columns[0])
    i = 1
    while i < len(df.columns):
        if i + 1 < len(df.columns):
            new_columns.append(df.columns[i] + ' - Value')
            new_columns.append(df.columns[i] + ' - Margin of Error')
        i += 2
    df.columns = new_columns

    df = df.replace(['#', '-', '!', '*', '~'], np.nan)
    df = df.dropna(how='all')

    local_authority_col = 'local authority: district / unitary (as of April 2021)'

    # Identify value and margin columns
    value_columns = [col for col in df.columns if 'Value' in col]

    # Create a mapping from 'Value' columns to their corresponding 'Margin of Error' columns
    value_to_margin_map = {
        value_col: value_col.replace('Value', 'Margin of Error')
        for value_col in value_columns
    }

    # Prepare an empty DataFrame to store the melted data
    melted_df = pd.DataFrame()

    # Melt the DataFrame for each pair of value and margin columns
    for value_col, margin_col in value_to_margin_map.items():
        temp_df = df[[local_authority_col, value_col, margin_col]].copy()
        temp_df.columns = ['local authority', 'value', 'margin of error']
        temp_df['measure'] = value_col.replace(' - Value', '')
        melted_df = pd.concat([melted_df, temp_df], ignore_index=True)

    melted_df = melted_df.reset_index(names='ID')
    melted_df = melted_df[['ID', 'local authority', 'measure', 'value', 'margin of error']]
    melted_df['value'] = melted_df['value'].apply(lambda x: float(x) / 100)
    melted_df['margin of error'] = melted_df['margin of error'].apply(lambda x: float(x) / 100)
    
    output_path = os.path.join(preprocessed_data_dir, 'economic_data.csv')
    melted_df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")




def grant360_data(
        downloaded_data_dir: str,
        preprocessed_data_dir: str
    ) -> None:

    logger.info(f"Pre-processing Grant360 data")
    data_dir = os.path.join(downloaded_data_dir, 'grant360')
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

    frames = []
    for file in files:
        df = pd.read_csv(os.path.join(data_dir,file))
        df.columns = [x.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(':', '_') for x in df.columns]
        frames.append(df)
    df = pd.concat(frames)
    
    df['award_date'] = df['award_date'].astype(str)
    df['award_date'] = df['award_date'].apply(lambda x: x.replace('/', '-'))
    df['award_date'] = df['award_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))

    output_path = os.path.join(preprocessed_data_dir, 'grant360_data.csv')
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



