select
    /* Primary Key */
    id

    /* Foreign Keys */
    , division.id as division_id
    , conference.id as conference_id
    , franchise.franchiseid as franchise_id

    /* Properties */
    , name as team_full_name
    , link as team_url
    , venue.name as venue_name
    , venue.link as venue_url
    , venue.city as venue_city
    , venue.timezone.id as venue_timezone_name
    , venue.timezone.offset as venue_timezone_offset
    , abbreviation as team_abbreviation
    , teamname as team_name
    , locationname as location_name
    , firstyearofplay as first_year_of_play
    , shortname as team_short_name
    , active as is_active

from {{ ref('teams') }}