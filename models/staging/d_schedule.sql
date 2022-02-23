SELECT
    /* Primary Key*/
    gamePK as id

    /* Foreign Keys*/
    , season as season_id
    , venue.id as venue_id
    , teams.home.team.id as home_team_id
    , teams.away.team.id as away_team_id

    /* Properties */
    , SUBSTR(CAST(gamePK AS STRING), 7, 4) as game_number
    , gameType as game_type
    , DATE(gameDate) as game_date
    , status.abstractGameState as game_state
    , status.statusCode as game_state_code
    , status.detailedState  as game_state_description
    , venue.name as venue_name
    , link as game_url

    FROM {{ ref('schedule') }}