select
    CAST(ladcd as text) as lad22cd,
    CAST(dimension as text) as dimension_name,
    CAST(mode_statement as text) as dimension_statement,
    CAST(average as float) as average,
    CAST(margin_of_error as float) as margin_of_error,
    CAST(n as integer) as response_n,
    CAST(evaluation_n as integer) as evaluation_n,
    CAST(organisation_n as integer) as organisation_n
from
    {{ source('preprocessed_data', 'raw__impact_and_insight_toolkit_local_authority_benchmarks') }}