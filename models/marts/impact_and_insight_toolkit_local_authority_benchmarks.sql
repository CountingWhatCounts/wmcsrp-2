select
    lad22cd,
    dimension_name,
    dimension_statement,
    average,
    margin_of_error,
    response_n,
    evaluation_n,
    organisation_n
from
    {{ ref('stg__impact_and_insight_toolkit_local_authority_benchmarks') }}