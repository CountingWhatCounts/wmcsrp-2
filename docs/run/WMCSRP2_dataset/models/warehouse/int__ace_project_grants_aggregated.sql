
  
  create view "WMCSRP2"."md_warehouse"."int__ace_project_grants_aggregated__dbt_tmp" as (
    with

project_grants as (
    select * from "WMCSRP2"."md_raw"."raw__ace_project_grants"
),


aggregated as (
    select
        sum(award_amount) as sum_award_amount,
        local_authority
    from
        project_grants
    group by
        local_authority
),


msoa_mapping as (
    select
        lad22cd,
        lad22nm,
        msoa21cd
    from
        "WMCSRP2"."md_warehouse"."int__area_codes"
),


combined as (
    select
        distinct lad22cd,
        lad22nm,
        case
            when sum_award_amount is null then 0
            else sum_award_amount
        end as sum_award_amount
    from msoa_mapping
    left join aggregated on msoa_mapping.lad22nm = aggregated.local_authority
)


select * from combined
  );
