with

msoa_codes as (
    select
        'msoa' as area_type,
        msoa21cd as area_code,
        msoa21nm as area_name,
        lad22cd as parent_area_code,
        lad22nm as parent_area_name
    from {{ ref('int__area_codes') }}
),

lad_codes as (
    select
        'local_authority' as area_type,
        lad22cd as area_code,
        lad22nm as area_name,
        rgn24cd as parent_area_code,
        rgn24nm as parent_area_name
    from {{ ref('int__area_codes') }}
),

region_codes as (
    select
        'region' as area_type,
        rgn24cd as area_code,
        rgn24nm as area_name,
        ctry24cd as parent_area_code,
        ctry24nm as parent_area_name
    from {{ ref('int__area_codes') }}
),

country_codes as (
    select
        'country' as area_type,
        ctry24cd as area_code,
        ctry24nm as area_name,
        null as parent_area_code,
        null as parent_area_name
    from {{ ref('int__area_codes') }}
),

combined as (
    select * from region_codes
    union all
    select * from country_codes
    union all
    select * from msoa_codes
    union all
    select * from lad_codes
)

select distinct * from combined