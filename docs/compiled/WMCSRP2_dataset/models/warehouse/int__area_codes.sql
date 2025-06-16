with

postcode_mapping as (
    select
        oslaua,
        ctry,
        rgn,
        msoa21cd
    from "wmcsrp2"."public_staging"."stg__postcode_mapping"
),

msoa_mapping as (
    select
        msoa21cd,
        msoa21nm,
        lad22cd,
        lad22nm
    from "wmcsrp2"."public_staging"."stg__msoa_mapping"
),

region_mapping as (
    select
        rgn24cd,
        rgn24nm
    from "wmcsrp2"."public_staging"."stg__region_mapping"
),

country_mapping as (
    select
        ctry24cd,
        ctry24nm
    from "wmcsrp2"."public_staging"."stg__country_mapping"
),

combined as (
    select
        p.msoa21cd,
        m.msoa21nm,
        m.lad22cd,
        m.lad22nm,
        r.rgn24cd,
        r.rgn24nm,
        c.ctry24cd,
        c.ctry24nm
    from postcode_mapping p
    left join msoa_mapping m on p.msoa21cd = m.msoa21cd
    left join region_mapping r on p.rgn = r.rgn24cd
    left join country_mapping c on p.ctry = c.ctry24cd
)

select * from combined where ctry24nm = 'England' and rgn24nm = 'West Midlands'