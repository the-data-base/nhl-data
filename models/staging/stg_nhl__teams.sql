select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['teams.id', 'teams.seasonid']) }} as stg_nhl__teams_id

    /* Identifiers */
    , teams.id as team_id
    , teams.venue.timezone.id as venue_timezone_id
    , teams.division.id as division_id
    , teams.conference.id as conference_id
    , teams.franchise.franchiseid as franchise_id
    , teams.seasonid as season_id

    /* Properties */
    , teams.name as full_name
    , teams.link as team_url
    , teams.venue.name as venue_name
    , teams.venue.link as venue_url
    , teams.venue.city as venue_city
    , teams.venue.timezone.offset as venue_timezone_offset
    , teams.venue.timezone.tz as venue_timezone_name
    , teams.abbreviation as abbreviation
    , teams.teamname as team_name
    , teams.locationname as location_name
    , teams.firstyearofplay as first_year_of_play
    , teams.division.name as division_name
    , teams.division.nameshort as division_short_name
    , teams.division.link as division_url
    , teams.division.abbreviation as division_abbreviation
    , teams.conference.name as conference_name
    , teams.conference.link as conference_url
    , teams.franchise.teamname as franchise_team_name
    , teams.franchise.link as franchise_url
    , teams.shortname as short_name
    , teams.officialsiteurl as official_site_url
    , teams.active as is_active
    , teams._time_extracted as extracted_at
    , teams._time_loaded as loaded_at
from {{ source('meltano', 'teams') }} as teams

{% if not use_full_dataset() %}
limit 1000
{% endif %}
