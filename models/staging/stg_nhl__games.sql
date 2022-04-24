with
linescore as (
    select * from {{ ref('stg_nhl__linescore') }}
)

, boxscore as (
    select * from {{ ref('stg_nhl__boxscore') }}
)

, final as (
    select
        /* Primary Key */
        {{ dbt_utils.surrogate_key(['linescore.id']) }} as id

        /* Foreign Keys */
        , linescore.game_id
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
        , boxscore.home_team_name
        , boxscore.home_team_pim
        , boxscore.home_team_shots
        , boxscore.home_team_powerplay_goals
        , boxscore.home_team_powerplay_opportunities
        , boxscore.home_team_faceoff_percentage
        , boxscore.home_team_blocked
        , boxscore.home_team_takeaways
        , boxscore.home_team_giveaways
        , boxscore.home_team_hits
        -- Away team stats
        , boxscore.away_team_name
        , boxscore.away_team_pim
        , boxscore.away_team_shots
        , boxscore.away_team_powerplay_goals
        , boxscore.away_team_powerplay_opportunities
        , boxscore.away_team_faceoff_percentage
        , boxscore.away_team_blocked
        , boxscore.away_team_takeaways
        , boxscore.away_team_giveaways
        , boxscore.away_team_hits
    from linescore
    left join boxscore
        on linescore.game_id = boxscore.game_id
            and linescore.home_team_id = boxscore.home_team_id
            and linescore.away_team_id = boxscore.away_team_id
)

select * from final
