SELECT
    content,
    date as year,
    geography,
    geography_code,
    n,
    measure,
    count
FROM
    {{ ref('seed_census') }}