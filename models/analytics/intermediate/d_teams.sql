select
    /* Primary Key */
    teams.id
    /* Foreign Keys */
    , teams.division_id
    , teams.conference_id
    , teams.franchise_id
    /* Teams Properties */
    , teams.full_name
    , teams.team_url
    , teams.venue_name
    , teams.venue_url
    , teams.venue_city
    , teams.venue_timezone_name
    , teams.venue_timezone_offset
    , teams.abbreviation
    , teams.team_name
    , teams.location_name
    , teams.first_year_of_play
    , teams.short_name
    , teams.is_active
from 
    {{ ref('stg_nhl__teams') }} as teams
