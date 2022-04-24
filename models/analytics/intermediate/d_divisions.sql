select
    /* Primary Key */
    divisions.id
    /* Foreign Keys */
    , divisions.conference_id
    /* Divisions Properties */
    , divisions.division_name
    , divisions.division_short_name
    , divisions.division_url
    , divisions.division_abbreviation
    , divisions.is_active

from {{ ref('stg_nhl__divisions') }} as divisions