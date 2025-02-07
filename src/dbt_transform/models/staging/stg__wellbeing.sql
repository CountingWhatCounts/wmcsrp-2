SELECT
    id,
    date,
    area_codes,
    measure,
    value,
    margin_of_error
FROM
    {{ ref('seed_wellbeing') }}
where
    date in ('April 2022 to March 2023')