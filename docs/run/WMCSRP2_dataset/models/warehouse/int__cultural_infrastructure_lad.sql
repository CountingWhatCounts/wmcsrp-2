
  
  create view "WMCSRP2"."md_warehouse"."int__cultural_infrastructure_lad__dbt_tmp" as (
    with

distinct_services as (
    select
        distinct service_name,
        lad22cd,
        service_type
    from "WMCSRP2"."md_warehouse"."int__cultural_infrastructure"
)

select
    lad22cd,
    service_type,
    count(service_name) as service_count
from distinct_services
group by lad22cd, service_type
  );
