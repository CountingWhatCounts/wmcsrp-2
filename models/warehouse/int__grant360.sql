with

postcodes as (
    select
        msoa21cd,
        postcode
    from
        {{ ref('stg__postcode_mapping')}}
),


area_codes as (
    select
        distinct lad22cd,
        msoa21cd
    from
        {{ ref('int__msoa_codes') }}
),


grant360 as (
    select
        amount_awarded,
        award_date,
        recipient_org_name,
        replace(recipient_org_postal_code, ' ', '') as recipient_org_postal_code,
        funding_org_name
    from
        {{ ref('stg__grant360_funding') }}
),


combined as (
    select
        distinct area_codes.msoa21cd,
        area_codes.lad22cd,
        grant360.amount_awarded,
        grant360.award_date,
        grant360.recipient_org_name,
        grant360.recipient_org_postal_code,
        grant360.funding_org_name
    from grant360
    join postcodes on grant360.recipient_org_postal_code = postcodes.postcode
    join area_codes on area_codes.msoa21cd = postcodes.msoa21cd
)


select * from combined