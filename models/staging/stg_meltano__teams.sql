select
    /* Primary Key */
    teams.id

    /* Foreign Keys */
    , teams.venue.timeZone.id as venue_timezone_id
    , teams.division.id as division_id
    , teams.conference.id as conference_id
    , teams.franchise.franchiseId as franchise_id

    /* Properties */
    , teams.name as full_name
    , teams.link as team_url
    , teams.venue.name as venue_name
    , teams.venue.link as venue_url
    , teams.venue.city as venue_city
    , teams.venue.timeZone.offset as venue_timezone_offset
    , teams.venue.timeZone.tz as venue_timezone_name
    , teams.abbreviation as abbreviation
    , teams.teamName as team_name
    , teams.locationName as location_name
    , teams.firstYearOfPlay as first_year_of_play
    , teams.division.name as division_name
    , teams.division.nameShort as division_short_name
    , teams.division.link as division_url
    , teams.division.abbreviation as division_abbreviation
    , teams.conference.name as conference_name
    , teams.conference.link as conference_url
    , teams.franchise.teamName as franchise_team_name
    , teams.franchise.link as franchise_url
    , teams.shortName as short_name
    , teams.officialSiteUrl as official_site_url
    , teams.active as is_active
    , teams._time_extracted as extracted_at
    , teams._time_loaded as loaded_at
from {{ source('meltano', 'teams') }} as teams
