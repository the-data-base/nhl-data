with

deduped as (
    select * from {{ source('meltano', 'schedule') }}
    qualify row_number() over (
        partition by
            gamepk
            , season
    ) = 1
)

select distinct
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['gamepk', 'season']) }} as stg_nhl__schedule_id

    /* Identifiers */
    , gamepk as game_id
    , season as season_id
    , teams.away.team.id as away_team_id
    , teams.home.team.id as home_team_id
    , venue.id as venue_id

    /* Properties */
    , substr(cast(gamepk as string), 7, 4) as game_number
    , substr(cast(gamepk as string), 5, 2) as game_type
    , case
        when substr(cast(gamepk as string), 5, 2) = '01' then 'Preseason'
        when substr(cast(gamepk as string), 5, 2) = '02' then 'Regular'
        when substr(cast(gamepk as string), 5, 2) = '03' then 'Playoffs'
        when substr(cast(gamepk as string), 5, 2) = '04' then 'All-star'
        else 'Unknown'
    end as game_type_description
    , date(gamedate) as game_date
    , status.abstractgamestate as abstract_game_state
    , status.codedgamestate as coded_game_state
    , status.detailedstate as detailed_state
    , status.statuscode as status_code
    , status.starttimetbd as is_start_time_tbd
    , teams.away.leaguerecord.wins as away_team_wins
    , teams.away.leaguerecord.losses as away_team_losses
    , teams.away.leaguerecord.ot as away_team_ot
    , teams.away.leaguerecord.type as away_team_type
    , teams.away.score as away_team_score
    , teams.away.team.name as away_team_name
    , teams.away.team.link as away_team_url
    , teams.home.leaguerecord.wins as home_team_wins
    , teams.home.leaguerecord.losses as home_team_losses
    , teams.home.leaguerecord.ot as home_team_ot
    , teams.home.leaguerecord.type as home_team_type
    , teams.home.score as home_team_score
    , teams.home.team.name as home_team_name
    , teams.home.team.link as home_team_url
    , venue.name as venue_name
    , venue.link as venue_url
    , content.link as content_url
    , link as url
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from deduped
