with

linescore as (
    select * from {{ ref('stg_nhl__linescore') }}
)

, boxscore_home_team as (
    select * from {{ ref('f_boxscore_team') }}
    where team_type = 'Home'
)

, boxscore_away_team as (
    select * from {{ ref('f_boxscore_team') }}
    where team_type = 'Away'
)

, final as (
    select
        /* Primary Key */
        linescore.game_id

        /* Identifiers */
        , linescore.home_team_id
        , linescore.away_team_id

        /* Properties */
        -- Game-level stats
        , linescore.game_score_description
        , linescore.game_matchup_description
        , linescore.game_winning_team_id
        , linescore.game_winning_team_name
        , linescore.game_absolute_goal_differential
        , linescore.home_team_goals
        , linescore.away_team_goals
        -- Home team stats
        , boxscore_home_team.team_name as home_team_name
        , boxscore_home_team.team_pim as home_team_pim
        , boxscore_home_team.team_shots as home_team_shots
        , boxscore_home_team.team_powerplay_goals as home_team_powerplay_goals
        , boxscore_home_team.team_powerplay_opportunities as home_team_powerplay_opportunities
        , boxscore_home_team.team_faceoff_percentage as home_team_faceoff_percentage
        , boxscore_home_team.team_blocked as home_team_blocked
        , boxscore_home_team.team_takeaways as home_team_takeaways
        , boxscore_home_team.team_giveaways as home_team_giveaways
        , boxscore_home_team.team_hits as home_team_hits
        -- Away team stats
        , boxscore_away_team.team_name as away_team_name
        , boxscore_away_team.team_pim as away_team_pim
        , boxscore_away_team.team_shots as away_team_shots
        , boxscore_away_team.team_powerplay_goals as away_team_powerplay_goals
        , boxscore_away_team.team_powerplay_opportunities as away_team_powerplay_opportunities
        , boxscore_away_team.team_faceoff_percentage as away_team_faceoff_percentage
        , boxscore_away_team.team_blocked as away_team_blocked
        , boxscore_away_team.team_takeaways as away_team_takeaways
        , boxscore_away_team.team_giveaways as away_team_giveaways
        , boxscore_away_team.team_hits as away_team_hits
    from linescore
    left join boxscore_home_team
        on linescore.game_id = boxscore_home_team.game_id
            and linescore.home_team_id = boxscore_home_team.team_id
    left join boxscore_away_team
        on linescore.game_id = boxscore_away_team.game_id
            and linescore.away_team_id = boxscore_away_team.team_id
)

select * from final
