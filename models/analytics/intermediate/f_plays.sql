--#TODO
select 
    *
from
    {{ ref('stg_meltano__live_plays') }}
