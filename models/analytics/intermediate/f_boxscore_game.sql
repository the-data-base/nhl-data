with

-- away team
boxscore_away_team as (
    select distinct
        id
        , team_id
        , team_goals
        , team_pim
        , team_shots
        , team_powerplay_goals
        , team_powerplay_opportunities
        , team_faceoff_percentage
        , team_blocked
        , team_takeaways
        , team_giveaways
        , team_hits
    from {{ ref('stg_meltano__boxscore') }}
    where team_type = 'Away'
)

-- home team
, boxscore_home_team as (
    select distinct
        id
        , team_id
        , team_goals
        , team_pim
        , team_shots
        , team_powerplay_goals
        , team_powerplay_opportunities
        , team_faceoff_percentage
        , team_blocked
        , team_takeaways
        , team_giveaways
        , team_hits
    from {{ ref('stg_meltano__boxscore') }}
    where team_type = 'Home'
)

select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['stg_meltano__boxscore.game_id', 'boxscore_away_team.team_id', 'boxscore_away_team.team_id']) }} as id

    /* Foreign Keys */
    , stg_meltano__boxscore.game_id
    , boxscore_away_team.team_id as away_team_id
    , boxscore_home_team.team_id as home_team_id

    /* Properties */
    , boxscore_away_team.team_goals as away_team_goals
    , boxscore_away_team.team_pim as away_team_pim
    , boxscore_away_team.team_shots as away_team_shots
    , boxscore_away_team.team_powerplay_goals as away_team_powerplay_goals
    , boxscore_away_team.team_powerplay_opportunities as away_team_powerplay_opportunities
    , boxscore_away_team.team_faceoff_percentage as away_team_faceoff_percentage
    , boxscore_away_team.team_blocked as away_team_blocked
    , boxscore_away_team.team_takeaways as away_team_takeaways
    , boxscore_away_team.team_giveaways as away_team_giveaways
    , boxscore_away_team.team_hits as away_team_hits
    , boxscore_home_team.team_goals as home_team_goals
    , boxscore_home_team.team_pim as home_team_pim
    , boxscore_home_team.team_shots as home_team_shots
    , boxscore_home_team.team_powerplay_goals as home_team_powerplay_goals
    , boxscore_home_team.team_powerplay_opportunities as home_team_powerplay_opportunities
    , boxscore_home_team.team_faceoff_percentage as home_team_faceoff_percentage
    , boxscore_home_team.team_blocked as home_team_blocked
    , boxscore_home_team.team_takeaways as home_team_takeaways
    , boxscore_home_team.team_giveaways as home_team_giveaways
    , boxscore_home_team.team_hits as home_team_hits

from
    {{ ref('stg_meltano__boxscore') }}
    left join boxscore_home_team on stg_meltano__boxscore.id = boxscore_home_team.id
    left join boxscore_away_team on stg_meltano__boxscore.id = boxscore_away_team.id

order by
    stg_meltano__boxscore.game_id desc
