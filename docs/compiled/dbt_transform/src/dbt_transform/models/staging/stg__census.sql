SELECT
    content,
    date as year,
    geography,
    geography_code,
    n,
    measure,
    count
FROM
    "dev"."main"."seed_census"