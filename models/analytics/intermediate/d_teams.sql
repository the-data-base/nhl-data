with deduplicated as (
    {{ dedupe(
        ref('stg_nhl__teams'),
        key_fields=['team_id'],
        sort_fields=['season_id']
    ) }}
)

select
    /* Primary Key */
    team_id

    /* Identifiers */
    , venue_timezone_id
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
from deduplicated
