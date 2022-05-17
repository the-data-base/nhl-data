select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['schedule.gamepk', 'schedule.season']) }} as stg_nhl__schedule_id

    /* Identifiers */
    , schedule.gamepk as game_id
    , schedule.season as season_id
    , schedule.teams.away.team.id as away_team_id
    , schedule.teams.home.team.id as home_team_id
    , schedule.venue.id as venue_id

    /* Properties */
    , substr(cast(schedule.gamepk as string), 7, 4) as game_number
    , substr(cast(gamepk as string), 5, 2) as game_type
    , case
        when substr(cast(gamepk as string), 5, 2) = '01' then 'Preseason'
        when substr(cast(gamepk as string), 5, 2) = '02' then 'Regular'
        when substr(cast(gamepk as string), 5, 2) = '03' then 'Playoffs'
        when substr(cast(gamepk as string), 5, 2) = '04' then 'All-star'
        else 'Unknown'
    end as game_type_description
    , date(schedule.gamedate) as game_date
    , schedule.status.abstractgamestate as abstract_game_state
    , schedule.status.codedgamestate as coded_game_state
    , schedule.status.detailedstate as detailed_state
    , schedule.status.statuscode as status_code
    , schedule.status.starttimetbd as is_start_time_tbd
    , schedule.teams.away.leaguerecord.wins as away_team_wins
    , schedule.teams.away.leaguerecord.losses as away_team_losses
    , schedule.teams.away.leaguerecord.ot as away_team_ot
    , schedule.teams.away.leaguerecord.type as away_team_type
    , schedule.teams.away.score as away_team_score
    , schedule.teams.away.team.name as away_team_name
    , schedule.teams.away.team.link as away_team_url
    , schedule.teams.home.leaguerecord.wins as home_team_wins
    , schedule.teams.home.leaguerecord.losses as home_team_losses
    , schedule.teams.home.leaguerecord.ot as home_team_ot
    , schedule.teams.home.leaguerecord.type as home_team_type
    , schedule.teams.home.score as home_team_score
    , schedule.teams.home.team.name as home_team_name
    , schedule.teams.home.team.link as home_team_url
    , schedule.venue.name as venue_name
    , schedule.venue.link as venue_url
    , schedule.content.link as content_url
    , schedule.link as url
    , schedule._time_extracted as extracted_at
    , schedule._time_loaded as loaded_at
from {{ source('meltano', 'schedule') }} as schedule
