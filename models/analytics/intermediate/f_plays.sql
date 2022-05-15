select
    /* Primary Key */
    stg_nhl__live_plays_id as play_id

    /* Identifiers */
    , game_id
    , event_idx
    , event_id
    , player_id
    , team_id

    /* Properties */
    , player_full_name
    , player_primary_assist
    , player_secondary_assist
    , player_role
    , player_role_team
    , event_type
    , event_code
    , event_description
    , event_secondary_type
    , penalty_severity
    , penalty_minutes
    , play_x_coordinate
    , play_y_coordinate
    , play_period
    , play_period_type
    , play_period_time_elapsed
    , play_period_time_remaining
    , play_period_seconds_elapsed
    , play_period_seconds_remaining
    , play_total_seconds_elapsed
    , play_total_seconds_remaining
    , play_time
    , shots_away
    , shots_home
    , hits_away
    , hits_home
    , faceoffs_away
    , faceoffs_home
    , takeaways_away
    , takeaways_home
    , giveaways_away
    , giveaways_home
    , missedshots_away
    , missedshots_home
    , blockedshots_away
    , blockedshots_home
    , penalties_away
    , penalties_home
    , first_goal_scored
    , last_goal_scored
    , goals_away
    , goals_home
    , goal_difference_current
    , winning_team_current
    , game_state_current
    , home_result_of_play
    , away_result_of_play
    , last_goal_game_winning
    , last_goal_game_tying
    , goals_home_lag
    , goals_away_lag
    , goal_difference_lag
    , winning_team_lag
    , game_state_lag
from {{ ref('stg_nhl__live_plays') }}

