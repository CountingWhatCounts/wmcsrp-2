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
    append_underscore_if_number,
)


def ace_project_grants(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing ACE Project Grants data")
    data_dir = os.path.join(downloaded_data_dir, "ace_project_grants")
    files = [f for f in os.listdir(data_dir) if f.endswith(".xlsx")]

    df = pd.read_excel(
        os.path.join(data_dir, files[0]), sheet_name="Project Grants Awards"
    )
    df.columns = df.iloc[1, :]
    df = df.iloc[2:, :]
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def ace_npo_funding(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing ACE NPO Funding data")
    data_dir = os.path.join(downloaded_data_dir, "ace_npo_funding")
    files = [f for f in os.listdir(data_dir) if f.endswith(".xlsx")]

    df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name="data")
    df.columns = [
        x.lower()
        .replace(" ", "_")
        .replace("\n", "_")
        .replace("/", "_")
        .replace("(", "_")
        .replace(")", "_")
        for x in df.columns
    ]

    df["applicant_name"] = df["applicant_name"].astype(str)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def economic_data(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Annual Survey Economic data")
    data_dir = os.path.join(downloaded_data_dir, "economic")
    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    pd.set_option("future.no_silent_downcasting", True)

    df = pd.read_csv(os.path.join(data_dir, files[0]))
    df.columns = df.loc[5, :]
    df = df.loc[7:, :]

    df = filter_columns(df)

    new_columns = []
    new_columns.append(df.columns[0])
    i = 1
    while i < len(df.columns):
        if i + 1 < len(df.columns):
            new_columns.append(df.columns[i] + " - Value")
            new_columns.append(df.columns[i] + " - Margin of Error")
        i += 2
    df.columns = new_columns

    df = df.replace(["#", "-", "!", "*", "~"], np.nan)
    df = df.dropna(how="all")

    local_authority_col = "local authority: district / unitary (as of April 2021)"

    # Identify value and margin columns
    value_columns = [col for col in df.columns if "Value" in col]

    # Create a mapping from 'Value' columns to their corresponding 'Margin of Error' columns
    value_to_margin_map = {
        value_col: value_col.replace("Value", "Margin of Error")
        for value_col in value_columns
    }

    # Prepare an empty DataFrame to store the melted data
    melted_df = pd.DataFrame()

    # Melt the DataFrame for each pair of value and margin columns
    for value_col, margin_col in value_to_margin_map.items():
        temp_df = df[[local_authority_col, value_col, margin_col]].copy()
        temp_df.columns = ["local authority", "value", "margin of error"]
        temp_df["measure"] = value_col.replace(" - Value", "")
        melted_df = pd.concat([melted_df, temp_df], ignore_index=True)

    melted_df = melted_df.reset_index(names="ID")
    melted_df = melted_df[
        ["ID", "local authority", "measure", "value", "margin of error"]
    ]
    melted_df["value"] = melted_df["value"].apply(lambda x: float(x) / 100)
    melted_df["margin of error"] = melted_df["margin of error"].apply(
        lambda x: float(x) / 100
    )
    melted_df.columns = [x.lower().replace(" ", "_") for x in melted_df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    melted_df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def giving360_data(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Grant360 data")
    data_dir = os.path.join(downloaded_data_dir, "grant360")
    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

    frames = []
    for file in files:
        df = pd.read_csv(os.path.join(data_dir, file))
        df.columns = [
            x.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(":", "_")
            for x in df.columns
        ]
        frames.append(df)
    df = pd.concat(frames)

    df["recipient_org_company_number"] = df["recipient_org_company_number"].astype(str)

    df["award_date"] = df["award_date"].astype(str)
    df["award_date"] = df["award_date"].apply(lambda x: x.replace("/", "-"))
    df["award_date"] = df["award_date"].apply(
        lambda x: pd.to_datetime(x).strftime("%Y-%m-%d")
    )

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def imd_data(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Indices of Deprivation data")
    data_dir = os.path.join(downloaded_data_dir, "indices_of_deprivation")
    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

    df = pd.read_csv(os.path.join(data_dir, files[0]))
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def cultural_infrastructure(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Cultural Infrastructure data")
    data_dir = os.path.join(downloaded_data_dir, "services")
    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

    frames = []
    for file in files:
        df = pd.read_csv(os.path.join(data_dir, file))
        df.columns = [
            x.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(":", "_")
            for x in df.columns
        ]
        frames.append(df)
    df = pd.concat(frames)
    df = df.drop("amount_awarded", axis=1)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def wellbeing(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Wellbeing data")
    data_dir = os.path.join(downloaded_data_dir, "wellbeing")
    files = [f for f in os.listdir(data_dir) if f.endswith(".xlsx")]

    sheets = {
        "1 Life satisfaction means": 11,
        "4 Worthwhile means": 11,
        "7 Happiness means": 11,
        "10 Anxiety means": 12,
    }

    sheet_frames = []

    for sheet, columns_row in sheets.items():
        df = pd.read_excel(os.path.join(data_dir, files[0]), sheet_name=sheet)
        df.columns = df.iloc[columns_row, :]
        df = df.iloc[columns_row + 1 :, 1:]

        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace("[cv1]", "<5%")
        df = df.replace("[cv2]", "5-10%")
        df = df.replace("[cv3]", "10-20%")
        df = df.replace("[cv4]", ">20%")
        df = df.replace("[u]", np.nan)
        df = df.replace("[x]", np.nan)

        index_column = "Area Codes"

        melted_df = pd.DataFrame()
        i = 1
        while i < len(df.columns):
            value_column = df.columns[i]
            error_column = df.columns[i + 1]
            temp_df = df[[index_column, value_column, error_column]].copy()
            temp_df["date"] = value_column
            temp_df.columns = ["area codes", "value", "margin of error", "date"]
            melted_df = pd.concat([melted_df, temp_df], ignore_index=True)
            i += 2

        melted_df["measure"] = sheet
        sheet_frames.append(melted_df)
    df = pd.concat(sheet_frames)

    df["measure"] = df["measure"].apply(lambda x: " ".join(x.split(" ")[1:]))
    df["value"] = df["value"].astype(float)
    df = df.reset_index(names="ID")
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def census_data(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Census data")
    data_dir = os.path.join(downloaded_data_dir, "census")
    files = [f for f in os.listdir(data_dir) if f.endswith(".xlsx")]

    frames = []
    for folder in get_data_folders(data_dir):
        files = [
            f for f in os.listdir(os.path.join(data_dir, folder)) if f.endswith(".csv")
        ]

        for filename in files:
            geography = get_geography_from_filename(filename)

            # Only process MSOA data
            if geography != "msoa":
                continue

            try:
                df = pd.read_csv(os.path.join(data_dir, folder, filename))
            except pd.errors.EmptyDataError:
                logger.info(f"No data present in {filename}.")
                continue

            # If age data then preprocess
            if "ts007" in filename:
                df = process_age_data(df)

            total_column = get_total_column_name(df)
            data_content = get_data_content_from_total_column(total_column)
            value_columns = get_value_columns(df, total_column)

            df_totals = df[
                ["date", "geography", "geography code", total_column]
            ].drop_duplicates()
            df_values = df[["geography code"] + value_columns]
            df_values = df_values.melt(
                id_vars="geography code", var_name="measure", value_name="count"
            )

            df = df_totals.merge(df_values, how="left", on="geography code")
            df = df.rename(columns={total_column: "n"})

            df["content"] = data_content
            df["geography"] = geography
            df = df[df["measure"] != total_column]
            df["measure"] = df["measure"].apply(remove_content_from_measure_name)
            frames.append(df)

    df = pd.concat(frames)
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def yougov(downloaded_data_dir: str, seed_data_dir: str, output_filename: str) -> None:
    logger.info("Pre-processing YouGov Survey data")
    data_dir = os.path.join(downloaded_data_dir, "yougov")

    _, meta = pyreadstat.read_sav(
        os.path.join(
            data_dir, "SAV for Indigo (Local Authorities 2024) 232 10.1.2025.sav"
        )
    )
    df = pd.read_excel(
        os.path.join(
            data_dir,
            "SAV for Indigo (Local Authorities 2024) 232 10.1.2025 - LABEL.xlsx",
        )
    )

    df = df.rename(columns=meta.column_names_to_labels)
    df = df.dropna(how="all")
    df["RecordNo"] = df["RecordNo"].astype(int)

    df = replace_invalid_characters(df)
    df = label_duplicate_columns(df)
    df = append_underscore_if_number(df)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def rural_urban_classification(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Rural Urban Classification data")
    data_dir = os.path.join(downloaded_data_dir, "rural_urban_classification")

    df = pd.read_csv(
        os.path.join(
            data_dir,
            "Rural_Urban_Classification_(2011)_of_Middle_Layer_Super_Output_Areas_in_England_and_Wales.csv",
        )
    )

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def ace_priority_places(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing ACE Priority Places data")
    data_dir = os.path.join(downloaded_data_dir, "ace_priority_places")

    df = pd.read_csv(os.path.join(data_dir, "ace_priority_places.csv"))
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def ace_levelling_up_places(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing ACE Levelling Up for Culture Places data")
    data_dir = os.path.join(downloaded_data_dir, "levelling_up_places")

    df = pd.read_excel(
        os.path.join(data_dir, "levelling_up_places.xlsx"),
        sheet_name="Sheet1",
        engine="openpyxl",
    )
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def postcode_mapping(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Postcode Mapping data")
    data_dir = os.path.join(downloaded_data_dir, "postcode_mapping")

    df = pd.read_csv(os.path.join(data_dir, "ONSPD_NOV_2024_UK.csv"))
    df = df.replace(r"^\s*$", np.nan, regex=True)
    df.columns = [x.lower() for x in df.columns]

    df["streg"] = df["streg"].astype(str)
    df["ur01ind"] = df["ur01ind"].astype(str)
    df["ru11ind"] = df["ru11ind"].astype(str)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def msoa_mapping(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing MSOA Mapping data")
    data_dir = os.path.join(downloaded_data_dir, "msoa_mapping")

    df = pd.read_csv(
        os.path.join(
            data_dir,
            "MSOA_(2011)_to_MSOA_(2021)_to_Local_Authority_District_(2022)_Lookup_for_England_and_Wales_-5379446518771769392.csv",
        )
    )
    df.columns = [x.lower() for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def msoa_population(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing MSOA Population data")
    data_dir = os.path.join(downloaded_data_dir, "msoa_population")

    df = pd.read_excel(
        os.path.join(data_dir, "sapemsoasyoatablefinal.xlsx"),
        sheet_name="Mid-2021 MSOA 2021",
    )
    df.columns = df.iloc[2, :]
    df = df.iloc[3:, :]
    df = df[["MSOA 2021 Code", "Total"]]
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def impact_and_insight_toolkit(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Impact & Insight Toolkit data")
    data_dir = os.path.join(
        downloaded_data_dir, "impact_and_insight_toolkit_local_authority_benchmarks"
    )

    df = pd.read_csv(os.path.join(data_dir, "iit_lad_dimension_benchmarks.csv"))
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def participation_survey_data(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Participation Survey data")
    data_dir = os.path.join(downloaded_data_dir, "participation_survey")
    df = pd.read_csv(
        os.path.join(data_dir, "participation_2023-24_annual_data_open.tab"),
        sep="\t",
        encoding="cp1252",
    )

    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = df[col].str.encode("utf-8", errors="strict").str.decode("utf-8")
        except Exception as e:
            print(f"Column {col} has problematic characters: {e}")

    int32_cols = ["ArchYcserial", "ArchIndivSerial", "ArchHHSerial"]
    df[int32_cols] = df[int32_cols].astype("int32")

    float32_cols = [
        "rimweightPS_trim2",
        "finalweight",
        "rimweightwebPS_trim2",
        "finalweightweb",
        "Y3GrossingWeight",
        "Y3GrossingWeight_WebOnly",
        "Y3SampleSizeWeight",
        "Y3SampleSizeWeight_WebOnly",
    ]
    df[float32_cols] = df[float32_cols].map(
        lambda x: x if isinstance(x, (float, int)) else np.nan
    )
    df[float32_cols] = df[float32_cols].astype("Float32")

    category_cols = ["gor11nm", "gor11cd", "LAU121CD"]
    df[category_cols] = df[category_cols].astype("category")

    int16_cols = [
        col
        for col in df.columns
        if col not in int32_cols + float32_cols + category_cols
    ]
    df[int16_cols] = df[int16_cols].map(lambda x: x if isinstance(x, int) else pd.NA)
    df[int16_cols] = df[int16_cols].astype("Int16")

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def participation_survey_variable_dictionary(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Participation Survey variables dictionary")
    data_dir = os.path.join(downloaded_data_dir, "participation_survey")

    _, meta = pyreadstat.read_sav(
        os.path.join(data_dir, "participation_2023-24_annual_data_open.sav"),
        metadataonly=True,
    )

    df = pd.DataFrame(
        {
            "variable_name": meta.column_names,
            "variable_label": meta.column_labels,
        }
    )

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def participation_survey_values_dictionary(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Participation Survey values dictionary")
    data_dir = os.path.join(downloaded_data_dir, "participation_survey")

    _, meta = pyreadstat.read_sav(
        os.path.join(data_dir, "participation_2023-24_annual_data_open.sav"),
        metadataonly=True,
    )

    value_map = {
        var: meta.variable_value_labels.get(var)
        for var in meta.column_names
        if meta.variable_value_labels.get(var)
    }

    rows = []
    for variable, mapping in value_map.items():
        for key, value in mapping.items():
            rows.append({"variable": variable, "key": key, "value": value})
    df = pd.DataFrame(rows)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def community_life_survey(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    def process_community_life_sheet(df: pd.DataFrame):
        header_row = 6
        data_row = 7

        if df.iloc[header_row, 0] != "Question":
            raise ValueError(
                f"Sheet configuration incorrect, header check value: {df.iloc[header_row, 0]}"
            )

        df.columns = df.iloc[header_row, :]
        data = df.iloc[data_row:, :].reset_index(drop=True)
        data = data.rename(columns={"Response Breakdown ": "Local Authority"})
        data = data.drop(["Question"], axis=1)
        data.iloc[0, 1] = "All"
        data = data.iloc[:, :8]

        return data

    logger.info("Pre-processing Community Life Survey data")
    data_dir = os.path.join(downloaded_data_dir, "community_life_survey")
    processing_spec = {
        "A1c": {
            "description": "Frequency of feelings of loneliness, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How often do you feel lonely?: Often/Always",
        },
        "A3c": {
            "description": "Indirect Loneliness Composite Score, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Indirect Loneliness: Score of 8 or 9",
        },
        "B1c": {
            "description": "Strength of feelings of belonging to immediate neighbourhood, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How strongly do you feel you belong to your immediate neighbourhood?: Total",
        },
        "B2c": {
            "description": "Perception of change to local area over the past two years, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Do you think that over the past two years your area has…: Got better to live in",
        },
        "B3c": {
            "description": "Extent of agreement that people in the neighbourhood pull together to improve the neighbourhood, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "To what extent would you agree or disagree that people in your neighbourhood pull together to improve the neighbourhood?: Agree",
        },
        "B4c": {
            "description": "Extent of agreement that people in the neighbourhood can be trusted, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Thinking about the people who live in this neighbourhood, to what extent do you believe they can be trusted?: Many of the people can be trusted",
        },
        "B5c": {
            "description": "Extent of agreement that you can personally influence decisions affecting the local area, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "To what extent do you agree or disagree that you personally can influence decisions affecting your local area?: Agree",
        },
        "B6c": {
            "description": "Extent of agreement that your local area is a place where people from different backgrounds get on well together, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "To what extent do you agree or disagree that this local area is a place where people from different backgrounds get on well together?: Agree",
        },
        "B7c": {
            "description": "Satisfaction with green and natural spaces in the local area, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How satisfied or dissatisfied are you with the green and natural spaces in your local area? : Satisfied",
        },
        "B10c": {
            "description": "Levels of agreement that you are proud to live in your local area, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How much do you agree or disagree with the following statements? - I am proud to live in my local area : Agree",
        },
        "B11c": {
            "description": "Levels of agreement that in five years you would still like to be living in the local area, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How much do you agree or disagree with the following statements? - In five years’ time I would like to still be living in my local area: Agree",
        },
        "B12c": {
            "description": "Levels of agreement that you would recommend your local area to others as a good place to live, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How much do you agree or disagree with the following statements? - I would recommend my local area to others as a good place to live: Agree",
        },
        "B15c": {
            "description": "Satisfaction with local area as a place to live, by local authority, people aged 16 and over, England, January to March 2024",
            "metric": "Overall, how satisfied or dissatisfied are you with your local area as a place to live? : Satisfied",
        },
        "B16c": {
            "description": "Extent of agreement that if you needed help there would be people there for you, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "To what extent do you agree or disagree that 'if I needed help, there are people who would be there for me'?: Agree",
        },
        "B17c": {
            "description": "Extent of agreement that if you wanted company there would be people to call on, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "To what extent do you agree or disagree that 'if I wanted company or to socialise, there are people I can call on'?: Agree",
        },
        "B18c": {
            "description": "Number of people who you can really count on to listen to you when you need to talk, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Is there anyone who you can really count on to listen to you when you need to talk?: Yes",
        },
        "B19c": {
            "description": "Attractiveness of local area, by local authority, people aged 16 and over England, October 2023 to March 2024",
            "metric": "Attractiveness of local area: Agree",
        },
        "C1c(A)": {
            "description": "Formal Volunteering at least once a month in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Formal volunteering: At least once in the last month",
        },
        "C1c(B)": {
            "description": "Formal Volunteering at least once in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Formal volunteering: At least once in the last 12 months",
        },
        "C1c(C)": {
            "description": "Informal Volunteering at least once a month in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Informal volunteering: At least once in the last month",
        },
        "C1c(D)": {
            "description": "Informal Volunteering at least once in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Informal volunteering: At least once in the last 12 months",
        },
        "C1c(E)": {
            "description": "Any Volunteering at least once a month in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Any volunteering: At least once in the last month",
        },
        "C1c(F)": {
            "description": "Any Volunteering at least once in the last 12 months, by local authority, England, October 2023 to March 2024",
            "metric": "Any volunteering: At least once in the last 12 months",
        },
        "C4c": {
            "description": "Percentage who have given to charitable causes in the last four weeks, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Gave to charitable causes in the last four weeks: Total",
        },
        "C6c": {
            "description": "Frequency of chatting to neighbours more than once a month, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Frequency of chatting to neighbours more than to just say hello: At least once a month",
        },
        "X1c": {
            "description": "Engagement in civic participation activities in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Civic participation at least once in the last 12 months: Total",
        },
        "X2c": {
            "description": "Engagement in civic activism in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Civic activism at least once in the last 12 months: Total",
        },
        "X9c": {
            "description": "Engagement in civic participation, activism or consultation activities in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Civic engagement at least once in the last 12 months: Total",
        },
        "X3c": {
            "description": "Engagement in civic consultation in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Civic consultation at least once in the last 12 months: Total",
        },
        "X8c": {
            "description": "Involvement in social action in the last 12 months, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Social action at least once in last 12 months : Total",
        },
        "X4c": {
            "description": "Importance of personally feeling that you can influence decisions in your local area, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "How important is it for you personally to feel that you can influence decisions in your local area?: Important",
        },
        "X5c": {
            "description": "Whether people would like to be more involved in local decisions, by local authority, people aged 16 and over, England, October 2023 to March 2024",
            "metric": "Generally speaking, would you like to be more involved in the decisions your council makes that affect your local area?: Yes",
        },
    }

    frames = []
    for sheet, info in processing_spec.items():
        df = pd.read_excel(
            os.path.join(
                data_dir,
                "Community_Life_Survey_2023_24_Annual_tables_-_for_publication.xlsx",
            ),
            sheet_name=sheet,
        )
        df = process_community_life_sheet(df=df)
        df.insert(0, "Metric", info.get("metric"))
        frames.append(df)
    df = pd.concat(frames)
    df = df[df["2023/24 Percentage (%)"] != "All"]

    df.columns = [
        x.lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(":", "_")
        .replace("%", "")
        .replace("/", "_")
        for x in df.columns
    ]

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def modelled_participation_statistics(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Modelled Participation Statistics")
    data_dir = os.path.join(downloaded_data_dir, "participation_survey")

    def process_sheet(input, column_name):
        df = input.copy()
        df = df.dropna(axis=1, how="all")

        row_A = df.loc[3, :].ffill(axis=0).astype(str).str.strip()
        row_B = df.loc[4, :].astype(str).str.strip()

        df.loc[4, :] = row_A + ": " + row_B
        df.loc[4, :] = df.loc[4, :].apply(lambda x: x.replace("nan: ", ""))

        df.columns = df.loc[4, :]
        df = df.rename(
            columns={
                "Has Not Physically Engaged With Events": "Not Engaged",
                "Has Physically Engaged With Events": "Engaged",
                "Engagement With Digital Activities": "Engaged",
                "No Engagment With Digital Activities": "Not Engaged",
                "Not Participated With Creative Activities": "Not Engaged",
                "Participated With Creative Activities": "Engaged",
            }
        )
        df = df.drop(["Not Engaged", "Local Authority Name"], axis=1)

        df = df.loc[5:, :]

        multiply_columns = [
            col for col in df.columns if col not in ("LAD23CD", "Engaged")
        ]
        df[multiply_columns] = df[multiply_columns].apply(
            pd.to_numeric, errors="coerce"
        )
        df[multiply_columns] = df[multiply_columns].multiply(df["Engaged"], axis=0)

        df = df.rename(columns={"Engaged": "Total Engaged"})
        df = df.melt(id_vars="LAD23CD", var_name="demographic", value_name="value")

        df["participation_type"] = column_name

        df = df[
            [
                "LAD23CD",
                "participation_type",
                "demographic",
                "value",
            ]
        ]

        return df

    sheets = [
        "A1 - Physical Engagement",
        "A2 - Exhibitions",
        "A3 - Theatre",
        "A4 - Literature Event",
        "A5 - Cinema",
        "A6 - Craft Exhibition",
        "A7 - Live Music ",
        "A8 - Arts Festival",
        "A9 - Street Art Event",
        "A10 - Live Dance",
        "A11 - Fashion Show",
        "A12 - Comedy Event",
        "A13 - Esports Event",
        "A14 - Other Cultural Event",
        "A15 - No Events",
        "A16 - Written Stories",
        "A17 - Reading Books",
        "A18 - Written Music",
        "A19 - Painting",
        "A20 - Crafts",
        "A21 - Choreographed",
        "A22 - Designed Video Games",
        "A23 - Made Films",
        "A24 - Photography",
        "A25 - Reading News",
        "A26 - Other Arts",
        "A27 - Not Participated",
        "B1 - Digital Engagement",
        "B2 - e-book",
        "B3 - Read News",
        "B4 - Video Games",
        "B5 - Live TV",
        "B6 - Streaming Service TV",
        "B7 - Live Films",
        "B8 - Streaming Service Films",
        "B9 - Live Radio",
        "B10 - Streamed Music",
        "B11 - Downloaded Music",
        "B12 - Audiobook",
        "B13 - Podcast",
        "B14 - No Digital Activities",
        "B15 - Live Arts",
        "B16 - Pre-recorded Arts",
        "B17 - Live Music or Dance",
        "B18 - Pre-recorded Music",
        "B19 - None",
    ]

    frames = []
    for sheet in sheets:
        df = pd.read_excel(
            os.path.join(
                data_dir,
                "WM Participation Survey Breakdown - Physical and Digital Participation.xlsx",
            ),
            sheet_name=sheet,
        )
        column_name = sheet.split(" - ")[1]
        df = process_sheet(input=df, column_name=column_name)
        frames.append(df)
    df = pd.concat(frames)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")
