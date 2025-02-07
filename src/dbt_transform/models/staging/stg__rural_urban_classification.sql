SELECT
    MSOA11CD as msoa11cd,
    MSOA11NM as msoa11nm,
    RUC11CD as ruc11cd,
    RUC11 as ruc11
FROM
    {{ ref('seed_rural_urban_classification') }}