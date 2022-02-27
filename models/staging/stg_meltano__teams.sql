select
    /* Primary Key */
    id

    /* Foreign Keys */
    , venue.timeZone.id as venue_timezone_id
    , division.id as division_id
    , conference.id as conference_id
    , franchise.franchiseId as franchise_id

    /* Properties */
    , name as name
    , link as team_url
    , venue.name as venue_name
    , venue.link as venue_url
    , venue.city as venue_city
    , venue.timeZone.offset as venue_timezone_offset
    , venue.timeZone.tz as venue_timezone_name
    , abbreviation as abbreviation
    , teamName as team_name
    , locationName as location_name
    , firstYearOfPlay as first_year_of_play
    , division.name as division_name
    , division.nameShort as division_short_name
    , division.link as division_url
    , division.abbreviation as division_abbreviation
    , conference.name as conference_name
    , conference.link as conference_url
    , franchise.teamName as franchise_team_name
    , franchise.link as franchise_url
    , shortName as short_name
    , officialSiteUrl as official_site_url
    , active as is_active
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'teams') }}
