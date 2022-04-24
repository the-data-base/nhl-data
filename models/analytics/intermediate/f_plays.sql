--#TODO
select
    *
from
    {{ ref('stg_nhl__live_plays') }}
