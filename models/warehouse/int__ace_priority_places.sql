with

priority_places as (
    select * from {{ ref('raw__ace_priority_places') }}
),

local_authority_codes as (
    select * from {{ ref('int__local_authority_codes') }}
),

combined as (
    select
        lad22cd,
        lad22nm,
        priority_place
    from priority_places join local_authority_codes
    on priority_places.ladnm = local_authority_codes.lad22nm
)

select * from combined