
  
  create view "WMCSRP2"."main_warehouse"."int__cultural_infrastructure_msoa__dbt_tmp" as (
    with

distinct_services as (
    select
        distinct service_name,
        msoa21,
        service_type
    from "WMCSRP2"."main_warehouse"."int__cultural_infrastructure"
)

select
    msoa21,
    service_type,
    count(service_name) as service_count
from distinct_services
group by msoa21, service_type
  );
