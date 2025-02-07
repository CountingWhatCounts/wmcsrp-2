SELECT
    FID,
    MSOA11CD,
    MSOA11NM,
    RUC11CD,
    RUC11
FROM
    {{ ref('rural_urban_classification') }}