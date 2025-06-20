### pipeline/steps/preprocess.py
import os
from src import preprocessing as preprocess
from src.logger import logger
from pipeline.config import RAW_DATA_DIR, PREPROCESSED_DATA_DIR


data_spec = {
    # "raw__ace_npo_funding.parquet": preprocess.ace_npo_funding,
    "raw__economic.parquet": preprocess.economic_data,
    "raw__360giving.parquet": preprocess.giving360_data,
    "raw__indices_of_deprivation.parquet": preprocess.imd_data,
    "raw__cultural_infrastructure.parquet": preprocess.cultural_infrastructure,
    "raw__wellbeing.parquet": preprocess.wellbeing,
    "raw__census.parquet": preprocess.census_data,
    "raw__rural_urban_classification.parquet": preprocess.rural_urban_classification,
    "raw__ace_priority_places.parquet": preprocess.ace_priority_places,
    "raw__postcode_mapping.parquet": preprocess.postcode_mapping,
    "raw__msoa_mapping.parquet": preprocess.msoa_mapping,
    "raw__msoa_population.parquet": preprocess.msoa_population,
    "raw__ace_levelling_up_for_culture_places.parquet": preprocess.ace_levelling_up_places,
    "raw__participation_survey_data.parquet": preprocess.participation_survey_data,
    "raw__participation_survey_variable_dictionary.parquet": preprocess.participation_survey_variable_dictionary,
    "raw__participation_survey_values_dictionary.parquet": preprocess.participation_survey_values_dictionary,
    "raw__community_life_survey.parquet": preprocess.community_life_survey,
    "raw__community_life_survey_benchmarks.parquet": preprocess.community_life_survey_benchmarks,
    "raw__modelled_participation_statistics.parquet": preprocess.modelled_participation_statistics,
    "raw__dcms_participation_statistics.parquet": preprocess.participation_survey_dcms_data_tables,
    "raw__residents_survey_local_authority_results.parquet": preprocess.residents_survey_local_authority_results,
    "raw__region_populations.parquet": preprocess.region_populations,
    "raw__region_mapping.parquet": preprocess.region_mapping,
    "raw__country_mapping.parquet": preprocess.country_mapping,
    "raw__annual_household_income.parquet": preprocess.annual_household_income
}

def run():
    logger.info("======== PREPROCESSING DATA FILES ========")
    os.makedirs(PREPROCESSED_DATA_DIR, exist_ok=True)

    for file, processor in data_spec.items():
        output_path = os.path.join(PREPROCESSED_DATA_DIR, file)
        if not os.path.exists(output_path):
            processor(
                downloaded_data_dir=RAW_DATA_DIR,
                seed_data_dir=PREPROCESSED_DATA_DIR,
                output_filename=file,
            )
        else:
            logger.info(f"{file} already present")

    logger.info("======== PREPROCESSING COMPLETED ========\n\n")
