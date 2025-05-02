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

            if geography not in ["msoa", "rgn", "ltla"]:
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
            try:
                df["measure"] = df["measure"].apply(remove_content_from_measure_name)
            except Exception:
                df["measure"] = "Total"
                df["count"] = df["n"]
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
        os.path.join(data_dir, "participation_2023-24_annual_data_safeguard.tab"),
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
        os.path.join(data_dir, "participation_2023-24_annual_data_safeguard.sav"),
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
        os.path.join(data_dir, "participation_2023-24_annual_data_safeguard.sav"),
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


def community_life_survey_benchmarks(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Community Life Survey benchmarks")
    data_dir = os.path.join(downloaded_data_dir, "community_life_survey")

    def process_sheet(df: pd.DataFrame):
        df.columns = df.iloc[5, :]
        df = df.iloc[6:, :]
        df = df[df["Individual Breakdown"].isin(["Total", "West Midlands"])]
        df = df[df["Topic Breakdown"].isin(["Total", "Region (ITL1 level)"])]
        df["Individual Breakdown"] = df["Individual Breakdown"].replace(
            {"Total": "England"}
        )
        df = df.drop(["Response", "Topic Breakdown", "Question"], axis=1)
        df = df.rename(columns={"Individual Breakdown": "benchmark"})
        return df

    processing_spec = {
        "A1b": "How often do you feel lonely?: Often/Always",
        "A3b": "Indirect Loneliness: Score of 8 or 9",
        "B1b": "How strongly do you feel you belong to your immediate neighbourhood?: Total",
        "B2b": "Do you think that over the past two years your area has…: Got better to live in",
        "B3b": "To what extent would you agree or disagree that people in your neighbourhood pull together to improve the neighbourhood?: Agree",
        "B4b": "Thinking about the people who live in this neighbourhood, to what extent do you believe they can be trusted?: Many of the people can be trusted",
        "B5b": "To what extent do you agree or disagree that you personally can influence decisions affecting your local area?: Agree",
        "B6b": "To what extent do you agree or disagree that this local area is a place where people from different backgrounds get on well together?: Agree",
        "B7b": "How satisfied or dissatisfied are you with the green and natural spaces in your local area? : Satisfied",
        "B10b": "How much do you agree or disagree with the following statements? - I am proud to live in my local area : Agree",
        "B11b": "How much do you agree or disagree with the following statements? - In five years’ time I would like to still be living in my local area: Agree",
        "B12b": "How much do you agree or disagree with the following statements? - I would recommend my local area to others as a good place to live: Agree",
        "B15b": "Overall, how satisfied or dissatisfied are you with your local area as a place to live? : Satisfied",
        "B16b": "To what extent do you agree or disagree that 'if I needed help, there are people who would be there for me'?: Agree",
        "B17b": "To what extent do you agree or disagree that 'if I wanted company or to socialise, there are people I can call on'?: Agree",
        "B18b": "Is there anyone who you can really count on to listen to you when you need to talk?: Yes",
        "B19b": "Attractiveness of local area: Agree",
        "C1b(A)": "Formal volunteering: At least once in the last month",
        "C1b(B)": "Formal volunteering: At least once in the last 12 months",
        "C1b(C)": "Informal volunteering: At least once in the last month",
        "C1b(D)": "Informal volunteering: At least once in the last 12 months",
        "C1b(E)": "Any volunteering: At least once in the last month",
        "C1b(F)": "Any volunteering: At least once in the last 12 months",
        "C4b": "Gave to charitable causes in the last four weeks: Total",
        "C6b": "Frequency of chatting to neighbours more than to just say hello: At least once a month",
        "X1b": "Civic participation at least once in the last 12 months: Total",
        "X2b": "Civic activism at least once in the last 12 months: Total",
        "X9b": "Civic engagement at least once in the last 12 months: Total",
        "X3b": "Civic consultation at least once in the last 12 months: Total",
        "X8b": "Social action at least once in last 12 months : Total",
        "X4b": "How important is it for you personally to feel that you can influence decisions in your local area?: Important",
        "X5b": "Generally speaking, would you like to be more involved in the decisions your council makes that affect your local area?: Yes",
    }

    frames = []
    for sheet, metric in processing_spec.items():
        df = pd.read_excel(
            os.path.join(
                data_dir,
                "Community_Life_Survey_2023_24_Annual_tables_-_for_publication.xlsx",
            ),
            sheet_name=sheet,
        )
        df = process_sheet(df)
        df.insert(0, "metric", metric)
        frames.append(df)
    df = pd.concat(frames)

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

    df = pd.read_csv(
        os.path.join(
            data_dir,
            "modelled_participation_figures.csv",
        )
    )

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def modelled_participation_statistics_old(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Modelled Participation Statistics")
    data_dir = os.path.join(downloaded_data_dir, "participation_survey")

    def process_sheet(input, participation_type):
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
        # df[multiply_columns] = df[multiply_columns].multiply(df["Engaged"], axis=0)

        df = df.rename(columns={"Engaged": "Total Engaged"})
        df = df.melt(id_vars="LAD23CD", var_name="demographic", value_name="value")

        df["participation_type"] = participation_type

        df = df[
            [
                "LAD23CD",
                "participation_type",
                "demographic",
                "value",
            ]
        ]

        return df

    sheets = {
        "A1 - Physical Engagement": "Adult engagement with the arts in the last 12 months (physical) (Tables A2 to A14 and A16 to A26), April 2023 to March 2024",
        "A2 - Exhibitions": "Adult engagement with an exhibition of art, photography or sculptures, April 2023 to March 2024",
        "A3 - Theatre": "Adult engagement with a theatre play, drama, musical, pantomime, ballet or opera, April 2023 to March 2024",
        "A4 - Literature Event": "Adult engagement with an event connected with literature, books, reading, poetry reading or writing, April 2023 to March 2024",
        "A5 - Cinema": "Adult engagement with a cinema screening of a film or movie in the last 12 months, April 2023 to March 2024",
        "A6 - Craft Exhibition": "Adult engagement with a craft exhibition, April 2023 to March 2024",
        "A7 - Live Music ": "Adult engagement with a live music event, April 2023 to March 2024",
        "A8 - Arts Festival": "Adult engagement with an arts festival or carnival, April 2023 to March 2024",
        "A9 - Street Art Event": "Adult engagement with a street art event, April 2023 to March 2024",
        "A10 - Live Dance": "Adult engagement with a live dance event, April 2023 to March 2024",
        "A11 - Fashion Show": "Adult engagement with a fashion show, April 2023 to March 2024",
        "A12 - Comedy Event": "Adult engagement with a comedy event, April 2023 to March 2024",
        "A13 - Esports Event": "Adult engagement with an in-person esports contest or video game competition event, May 2023 to March 2024",
        "A14 - Other Cultural Event": "Adult engagement with some other cultural event in England, April 2023 to March 2024",
        "A15 - No Events": "Adults who have not attended any events, April 2023 to March 2024",
        "A16 - Written Stories": "Adult engagement with written stories, plays, or poetry, April 2023 to March 2024",
        "A17 - Reading Books": "Adult engagement with reading books, graphic novels or magazines, April 2023 to March 2024",
        "A18 - Written Music": "Adult engagement with written, practiced or performed music, April 2023 to March 2024",
        "A19 - Painting": "Adult engagement with painting, drawing, printmaking, calligraphy, colouring, April 2023 to March 2024",
        "A20 - Crafts": "Adult engagement with crafts (textile, sewing, ceramic, sculpting, carving, woodwork), April 2023 to March 2024",
        "A21 - Choreographed": "Adult engagement with choreographed or performed a drama or dance routine, April 2023 to March 2024",
        "A22 - Designed Video Games": "Adult engagement with designed or programmed video games including on a smartphone or tablet, April 2023 to March 2024",
        "A23 - Made Films": "Adult engagement with made films or videos including original video content and animations, April 2023 to March 2024",
        "A24 - Photography": "Adult engagement with photography as a hobby, April 2023 to March 2024",
        "A25 - Reading News": "Adult engagement with reading news in a printed newspaper, April 2023 to March 2024",
        "A26 - Other Arts": "Adult engagement with other arts, crafts, or creative activities, April 2023 to March 2024",
        "A27 - Not Participated": "Adults who have not participated, April 2023 to March 2024",
        "B1 - Digital Engagement": "Adult engagement with the arts in the last 12 months (digital) (Tables B2 to B13 and B15 to B18), April 2023 to March 2024",
        "B2 - e-book": "Adult engagement with reading an e-book or e-magazine, April 2023 to March 2024",
        "B3 - Read News": "Adult engagement with reading news online from a national or local news publisher, May 2023 to March 2024",
        "B4 - Video Games": "Adult engagement with playing video games including on a smartphone or tablet, May 2023 to March 2024",
        "B5 - Live TV": "Adult engagement with watching TV programmes live at the time they were broadcast, May 2023 to March 2024",
        "B6 - Streaming Service TV": "Adult engagement with watching TV programmes using a streaming service, May 2023 to March 2024",
        "B7 - Live Films": "Adult engagement with watching films live at the time they were broadcast, May 2023 to March 2024",
        "B8 - Streaming Service Films": "Adult engagement with watching films using a streaming service, May 2023 to March 2024",
        "B9 - Live Radio": "Adult engagement with listening to live radio online through a computer, laptop, tablet or phone, April 2023 to March 2024",
        "B10 - Streamed Music": "Adult engagement with listening to streamed music, April 2023 to March 2024",
        "B11 - Downloaded Music": "Adult engagement with listening to downloaded music, April 2023 to March 2024",
        "B12 - Audiobook": "Adult engagement with listening to an audiobook, April 2023 to March 2024",
        "B13 - Podcast": "Adult engagement with listening to a podcast, April 2023 to March 2024",
        "B14 - No Digital Activities": "Adult engagement with none of these activities, April 2023 to March 2024",
        "B15 - Live Arts": "Adult engagement with watching a live arts event including theatre, visual arts or literature online, April 2023 to March 2024",
        "B16 - Pre-recorded Arts": "Adult engagement with watching a pre-recorded arts event including theatre, visual arts or literature, April 2023 to March 2024",
        "B17 - Live Music or Dance": "Adult engagement with watching a live music or dance event, watched as it was happening, April 2023 to March 2024",
        "B18 - Pre-recorded Music": "Adult engagement with watching a pre-recorded music or dance event, April 2023 to March 2024",
        "B19 - None": "Adult engagement with none of these activities, April 2023 to March 2024",
    }

    frames = []
    for sheet, participation_type in sheets.items():
        df = pd.read_excel(
            os.path.join(
                data_dir,
                "WM Participation Survey Breakdown - Physical and Digital Participation.xlsx",
            ),
            sheet_name=sheet,
        )
        df = process_sheet(input=df, participation_type=participation_type)
        frames.append(df)
    df = pd.concat(frames)

    margin_of_error = pd.read_excel(
        os.path.join(
            data_dir,
            "WM Participation Survey Breakdown - Physical and Digital Participation.xlsx",
        ),
        sheet_name="Z2 - Sample Sizes",
    )
    margin_of_error.columns = margin_of_error.iloc[3, :]
    margin_of_error = margin_of_error.iloc[4:, :6].dropna().reset_index(drop=True)
    margin_of_error = margin_of_error[["LAD23CD", "Margin of Error"]]
    df = df.merge(margin_of_error, on="LAD23CD", how="left")

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def participation_survey_dcms_data_tables(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing DCMS Participation Statistics")
    data_dir = os.path.join(downloaded_data_dir, "participation_survey")

    sheets = [
        "Table_A2",
        "Table_A4",
        "Table_A6",
        "Table_A8",
        "Table_A10",
        "Table_A12",
        "Table_A14",
        "Table_A16",
        "Table_A18",
        "Table_A20",
        "Table_A22",
        "Table_A24",
        "Table_A26",
        "Table_A28",
        "Table_A30",
        "Table_A32",
        "Table_A34",
        "Table_A36",
        "Table_A38",
        "Table_A40",
        "Table_A42",
        "Table_A44",
        "Table_A46",
        "Table_A48",
        "Table_A50",
        "Table_A52",
        "Table_A54",
        "Table_B2",
        "Table_B4",
        "Table_B6",
        "Table_B8",
        "Table_B10",
        "Table_B12",
        "Table_B14",
        "Table_B16",
        "Table_B18",
        "Table_B20",
        "Table_B22",
        "Table_B24",
        "Table_B26",
        "Table_B28",
        "Table_B30",
        "Table_B32",
        "Table_B34",
        "Table_B36",
        "Table_B38",
        "Table_C2",
        "Table_C5",
        "Table_D2",
        "Table_D5",
        "Table_E2",
        "Table_E5",
    ]

    frames = []
    for sheet in sheets:
        df = pd.read_excel(
            os.path.join(
                data_dir,
                "Final_Revised_DCMS_Participation_Survey_Annual_23-24_Data_Tables_March_2025.xlsx",
            ),
            sheet_name=sheet,
        )
        df.columns = df.loc[2, :]
        df.insert(0, "Participation Type", df.loc[3, "Question"])
        df.loc[3, "Question"] = "Total"
        df = df.iloc[3:, :]
        df = df.rename(columns={"Question": "Response Group"})
        frames.append(df)
    df = pd.concat(frames)
    df = df.astype(str)
    df["Response Group"] = (
        df["Response Group"].str.replace(r"\[.*?\]", "", regex=True).str.strip()
    )
    df["Response Breakdown "] = (
        df["Response Breakdown "].str.replace(r"\[.*?\]", "", regex=True).str.strip()
    )

    cols = [
        "Percentage of respondents 2023/24",
        "Percentage of respondents 2023/24 Lower estimate",
        "Percentage of respondents 2023/24 Upper estimate",
        "2023/24 No. of respondents",
        "2023/24 Base",
        "Percentage of respondents 2022/23",
        "Percentage of respondents 2022/23 Lower estimate",
        "Percentage of respondents 2022/23 Upper estimate",
        "2022/23 No. of respondents",
        "2022/23 Base",
    ]
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def residents_survey_local_authority_results(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Residents Survey Local Authority Results")
    data_dir = os.path.join(downloaded_data_dir, "residents_survey")

    df = pd.read_csv(
        os.path.join(data_dir, "residents_survey_local_authority_results.csv")
    )

    df = df.melt(
        id_vars=["Question", "Answer"],
        var_name="local_authority",
        value_name="percentage",
    )
    left = df[df["Answer"] != "N"]
    right = df[df["Answer"] == "N"].drop("Answer", axis=1)
    df = left.merge(right, on=["Question", "local_authority"])
    df = df.rename(columns={"percentage_x": "percentage", "percentage_y": "n"})
    df["n"] = df["n"].astype(int)

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def region_populations(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Region Populations")
    data_dir = os.path.join(downloaded_data_dir, "region_populations")

    df = pd.read_csv(os.path.join(data_dir, "region_populations.csv"))

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def region_mapping(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Region Mapping")
    data_dir = os.path.join(downloaded_data_dir, "region_mapping")

    df = pd.read_csv(
        os.path.join(data_dir, "Regions_(December_2024)_Names_and_Codes_in_EN.csv")
    )

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def country_mapping(
    downloaded_data_dir: str, seed_data_dir: str, output_filename: str
) -> None:
    logger.info("Pre-processing Country Mapping")
    data_dir = os.path.join(downloaded_data_dir, "country_mapping")

    df = pd.read_csv(
        os.path.join(
            data_dir, "Countries_(December_2024)_Names_and_Codes_in_the_UK.csv"
        )
    )

    output_path = os.path.join(seed_data_dir, output_filename)
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")
