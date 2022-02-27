select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['gamepk']) }} as id

    /* Foreign Keys */
    , gamePk as game_id
    , season as season_id
    , teams.away.team.id as away_team_id
    , teams.home.team.id as home_team_id
    , venue.id as venue_id

    /* Properties */
    , substr(cast(gamePk as string), 7, 4) as game_number
    , link as url
    , gameType as game_type
    , date(gameDate) as game_date
    , status.abstractGameState as abstract_game_state
    , status.codedGameState as coded_game_state
    , status.detailedState as detailed_state
    , status.statusCode as status_code
    , status.startTimeTBD as is_start_time_tbd
    , teams.away.leagueRecord.wins as away_team_wins
    , teams.away.leagueRecord.losses as away_team_losses
    , teams.away.leagueRecord.ot as away_team_ot
    , teams.away.leagueRecord.type as away_team_type
    , teams.away.score as away_team_score
    , teams.away.team.name as away_team_name
    , teams.away.team.link as away_team_url
    , teams.home.leagueRecord.wins as home_team_wins
    , teams.home.leagueRecord.losses as home_team_losses
    , teams.home.leagueRecord.ot as home_team_ot
    , teams.home.leagueRecord.type as home_team_type
    , teams.home.score as home_team_score
    , teams.home.team.name as home_team_name
    , teams.home.team.link as home_team_url
    , venue.name as venue_name
    , venue.link as venue_url
    , content.link as content_url
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'schedule') }}
