select
    /* Primary Key*/
    gamepk as id

    /* Foreign Keys*/
    , season as season_id
    , venue.id as venue_id
    , teams.home.team.id as home_team_id
    , teams.away.team.id as away_team_id

    /* Properties */
    , substr(cast(gamepk as string), 7, 4) as game_number
    , gametype as game_type
    , date(gamedate) as game_date
    , status.abstractgamestate as game_state
    , status.statuscode as game_state_code
    , status.detailedstate as game_state_description
    , venue.name as venue_name
    , link as game_url

from {{ ref('schedule') }}