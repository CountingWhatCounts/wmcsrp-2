



with

postcodes as (
    select
        msoa21,
        pcd_no_space as postcode
    from
        "dev"."main_staging"."stg__postcode_mapping"
),


area_codes as (
    select
        lad22cd,
        lad22nm,
        msoa21cd,
    from
        "dev"."main_warehouse"."int__area_codes"
),


grant360 as (
    select
        amount_awarded,
        award_date,
        recipient_org_name,
        replace(recipient_org_postal_code, ' ', '') as recipient_org_postal_code,
        funding_org_name
    from
        "dev"."main_staging"."stg__grant360"
),


combined as (
    select
        distinct area_codes.msoa21cd,
        area_codes.lad22cd,
        area_codes.lad22nm,
        grant360.amount_awarded,
        grant360.award_date,
        grant360.recipient_org_name,
        grant360.recipient_org_postal_code,
        grant360.funding_org_name
    from grant360
    join postcodes on grant360.recipient_org_postal_code = postcodes.postcode
    join area_codes on area_codes.msoa21cd = postcodes.msoa21
)


select * from combined