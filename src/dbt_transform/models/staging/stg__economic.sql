select
    id,
    local_authority,
    measure,
    value,
    margin_of_error
from
    {{ ref('economic') }}