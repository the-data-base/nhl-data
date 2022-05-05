select
    /* Primary Key */
    conference_id

    /* Properties */
    , conference_name
    , conference_url
    , conference_abbreviation
    , conference_short_name
    , is_active
from {{ ref('stg_nhl__conferences') }}
