select
    /* Primary Key */
    id

    /* Foreign Keys */

    , division_id
    , conference_id
    , franchise_id

    /* Properties */
    , full_name
    , team_url
    , venue_name
    , venue_url
    , venue_city
    , venue_timezone_name
    , venue_timezone_offset
    , abbreviation
    , team_name
    , location_name
    , first_year_of_play
    , short_name
    , is_active
from {{ ref('stg_nhl__teams') }}
