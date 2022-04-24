select
    /* Primary Key */
    conferences.id
    /* Conferences Properties */
    , conferences.conference_name
    , conferences.conference_url
    , conferences.conference_abbreviation
    , conferences.conference_short_name
    , conferences.is_active
from {{ ref('stg_nhl__conferences') }} as conferences
