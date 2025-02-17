import pandas as pd
import numpy as np
import os
import pyreadstat

from .logger import logger
from .helper_functions import (
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
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing ACE Project Grants data")
    data_dir = os.path.join(downloaded_data_dir, 'ace_project_grants')
    files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name='Project Grants Awards')
    df.columns = df.iloc[1, :]
    df = df.iloc[2:, :]
    df.columns = [x.lower().replace(' ','_') for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def ace_npo_funding(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing ACE NPO Funding data")
    data_dir = os.path.join(downloaded_data_dir, 'ace_npo_funding')
    files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name='data')
    df.columns = [x.lower().replace(' ','_').replace('\n', '_').replace('/','_').replace('(','_').replace(')','_') for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def economic_data(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
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
    melted_df.columns = [x.lower().replace(' ','_') for x in melted_df.columns]
    
    output_path = os.path.join(seed_data_dir, output_filename)
    melted_df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def grant360_data(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
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

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def imd_data(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing Indices of Deprivation data")
    data_dir = os.path.join(downloaded_data_dir, 'indices_of_deprivation')
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

    df = pd.read_csv(os.path.join(data_dir, files[0]))
    df.columns = [x.lower().replace(' ', '_') for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def cultural_infrastructure(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing Cultural Infrastructure data")
    data_dir = os.path.join(downloaded_data_dir, 'services')
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

    frames = []
    for file in files:
        df = pd.read_csv(os.path.join(data_dir, file))
        df.columns = [x.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(':', '_') for x in df.columns]
        frames.append(df)
    df = pd.concat(frames)
    df = df.drop('amount_awarded', axis=1)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def wellbeing(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing Wellbeing data")
    data_dir = os.path.join(downloaded_data_dir, 'wellbeing')
    files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    sheets = {
        '1 Life satisfaction means': 11,
        '4 Worthwhile means': 11,
        '7 Happiness means': 11,
        '10 Anxiety means': 12
    }

    sheet_frames = []

    for sheet, columns_row in sheets.items():

        df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name=sheet)
        df.columns = df.iloc[columns_row,:]
        df = df.iloc[columns_row+1:,1:]

        df = df.replace('[cv1]', '<5%')
        df = df.replace('[cv2]', '5-10%')
        df = df.replace('[cv3]', '10-20%')
        df = df.replace('[cv4]', '>20%')
        df = df.replace('[u]', np.nan)
        df = df.replace('[x]', np.nan)

        index_column = 'Area Codes'

        melted_df = pd.DataFrame()
        i = 1
        while i < len(df.columns):
            value_column = df.columns[i]
            error_column = df.columns[i+1]
            temp_df = df[[index_column, value_column, error_column]].copy()
            temp_df['date'] = value_column
            temp_df.columns = ['area codes', 'value', 'margin of error', 'date']
            melted_df = pd.concat([melted_df, temp_df], ignore_index=True)
            i += 2

        melted_df['measure'] = sheet
        sheet_frames.append(melted_df)
    df = pd.concat(sheet_frames)

    df['measure'] = df['measure'].apply(lambda x: ' '.join(x.split(' ')[1:]))
    df['value'] = df['value'].astype(float)
    df = df.reset_index(names='ID')
    df.columns = [x.lower().replace(' ','_') for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def census_data(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing Census data")
    data_dir = os.path.join(downloaded_data_dir, 'census')
    files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    frames = []
    for folder in get_data_folders(data_dir):
        files = [f for f in os.listdir(os.path.join(data_dir, folder)) if f.endswith('.csv')]

        for filename in files:
            geography = get_geography_from_filename(filename)
            
            # Only process MSOA data
            if geography != 'msoa':
                continue
            
            try:
                df = pd.read_csv(os.path.join(data_dir, folder, filename))
            except pd.errors.EmptyDataError as e:
                logger.info(f'No data present in {filename}.')
                continue

            # If age data then preprocess
            if 'ts007' in filename:
                df = process_age_data(df)

            total_column = get_total_column_name(df)
            data_content = get_data_content_from_total_column(total_column)
            value_columns = get_value_columns(df, total_column)

            df_totals = df[['date', 'geography', 'geography code', total_column]].drop_duplicates()
            df_values = df[['geography code'] + value_columns]
            df_values = df_values.melt(
                id_vars='geography code',
                var_name='measure',
                value_name='count'
            )

            df = df_totals.merge(df_values, how='left', on='geography code')
            df = df.rename(columns={total_column: 'n'})

            df['content'] = data_content
            df['geography'] = geography
            df = df[df['measure']!=total_column]
            df['measure'] = df['measure'].apply(remove_content_from_measure_name)
            frames.append(df)

    df = pd.concat(frames)
    df.columns = [x.lower().replace(' ','_') for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def yougov(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing YouGov Survey data")
    data_dir = os.path.join(downloaded_data_dir, 'yougov')

    _, meta = pyreadstat.read_sav(os.path.join(data_dir,"SAV for Indigo (Local Authorities 2024) 232 10.1.2025.sav"))
    df = pd.read_excel(os.path.join(data_dir,"SAV for Indigo (Local Authorities 2024) 232 10.1.2025 - LABEL.xlsx"))

    df = df.rename(columns=meta.column_names_to_labels)
    df = df.dropna(how='all')
    df['RecordNo'] = df['RecordNo'].astype(int)

    df = replace_invalid_characters(df)
    df = label_duplicate_columns(df)
    df = append_underscore_if_number(df)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def rural_urban_classification(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing Rural Urban Classification data")
    data_dir = os.path.join(downloaded_data_dir, 'rural_urban_classification')

    df = pd.read_csv(os.path.join(
        data_dir,
        'Rural_Urban_Classification_(2011)_of_Middle_Layer_Super_Output_Areas_in_England_and_Wales.csv'
    ))

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def ace_priority_places(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing ACE Priority Places data")
    data_dir = os.path.join(downloaded_data_dir, 'ace_priority_places')

    df = pd.read_csv(os.path.join(data_dir,'ace_priority_places.csv'))
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def ace_levelling_up_places(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing ACE Levelling Up for Culture Places data")
    data_dir = os.path.join(downloaded_data_dir, 'levelling_up_places')

    df = pd.read_excel(os.path.join(data_dir,'levelling_up_places.xlsx'), sheet_name='Sheet1', engine='openpyxl')
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def postcode_mapping(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing Postcode Mapping data")
    data_dir = os.path.join(downloaded_data_dir, 'postcode_mapping')

    df = pd.read_csv(os.path.join(data_dir,'ONSPD_NOV_2024_UK.csv'))
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def msoa_mapping(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing MSOA Mapping data")
    data_dir = os.path.join(downloaded_data_dir, 'msoa_mapping')

    df = pd.read_csv(os.path.join(data_dir, 'MSOA_(2011)_to_MSOA_(2021)_to_Local_Authority_District_(2022)_Lookup_for_England_and_Wales_-5379446518771769392.csv'))
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")



def msoa_population(
        downloaded_data_dir: str,
        seed_data_dir: str,
        output_filename: str
    ) -> None:

    logger.info(f"Pre-processing MSOA Population data")
    data_dir = os.path.join(downloaded_data_dir, 'msoa_population')

    df = pd.read_excel(os.path.join(data_dir, 'sapemsoasyoatablefinal.xlsx'), sheet_name='Mid-2021 MSOA 2021')
    df.columns = df.iloc[2, :]
    df = df.iloc[3:, :]
    df = df[['MSOA 2021 Code', 'Total']]
    df.columns = [x.lower().replace(' ', '_') for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")