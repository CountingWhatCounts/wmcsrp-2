SELECT
    id,
    date,
    area_codes,
    measure,
    value,
    margin_of_error
FROM
    "WMCSRP2"."main_preprocessed_data"."seed_wellbeing"
where
    date in ('April 2022 to March 2023')