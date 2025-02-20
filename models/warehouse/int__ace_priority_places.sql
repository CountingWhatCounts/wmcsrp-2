with

priority_places as (
    select * from {{ ref('stg__ace_priority_places') }}
),

local_authority_codes as (
    select * from {{ ref('int__local_authority_codes') }}
),

combined as (
    select
        lad22cd,
        coalesce(priority_place, FALSE) as priority_place
    from local_authority_codes left join priority_places
    on priority_places.lad22nm = local_authority_codes.lad22nm
)

select * from combined