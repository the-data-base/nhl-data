select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['gamepk']) }} as id

    /* Foreign Keys */
    , schedule.gamePk as game_id
    , schedule.season as season_id
    , schedule.teams.away.team.id as away_team_id
    , schedule.teams.home.team.id as home_team_id
    , schedule.venue.id as venue_id

    /* Properties */
    , substr(cast(schedule.gamePk as string), 7, 4) as game_number
    , schedule.link as url
    , schedule.gameType as game_type
    , date(schedule.gameDate) as game_date
    , schedule.status.abstractGameState as abstract_game_state
    , schedule.status.codedGameState as coded_game_state
    , schedule.status.detailedState as detailed_state
    , schedule.status.statusCode as status_code
    , schedule.status.startTimeTBD as is_start_time_tbd
    , schedule.teams.away.leagueRecord.wins as away_team_wins
    , schedule.teams.away.leagueRecord.losses as away_team_losses
    , schedule.teams.away.leagueRecord.ot as away_team_ot
    , schedule.teams.away.leagueRecord.type as away_team_type
    , schedule.teams.away.score as away_team_score
    , schedule.teams.away.team.name as away_team_name
    , schedule.teams.away.team.link as away_team_url
    , schedule.teams.home.leagueRecord.wins as home_team_wins
    , schedule.teams.home.leagueRecord.losses as home_team_losses
    , schedule.teams.home.leagueRecord.ot as home_team_ot
    , schedule.teams.home.leagueRecord.type as home_team_type
    , schedule.teams.home.score as home_team_score
    , schedule.teams.home.team.name as home_team_name
    , schedule.teams.home.team.link as home_team_url
    , schedule.venue.name as venue_name
    , schedule.venue.link as venue_url
    , schedule.content.link as content_url
    , schedule._time_extracted as extracted_at
    , schedule._time_loaded as loaded_at
from {{ source('meltano', 'schedule') }} as schedule
