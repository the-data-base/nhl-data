select
    /* Primary Key */
    id

    /* Foreign Keys */
    , conference_id

    /* Properties */
    , division_name
    , division_name_short
    , division_url
    , division_abbreviation
    , is_active

from {{ ref('stg_meltano__divisions') }}
