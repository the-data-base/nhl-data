select
    /* Primary Key */
    stg_nhl__boxscore_team_id as boxscore_team_id

    /* Identifiers */
    , game_id
    , team_id

    /* Properties */
    , team_type
    -- team stats
    , team_name
    , team_winner
    , team_goals
    , team_goal_differential
    , team_pim
    , team_shots
    , team_powerplay_goals
    , team_powerplay_opportunities
    , team_faceoff_percentage
    , team_blocked
    , team_takeaways
    , team_giveaways
    , team_hits
from {{ ref('stg_nhl__boxscore_team') }}
order by game_id desc
