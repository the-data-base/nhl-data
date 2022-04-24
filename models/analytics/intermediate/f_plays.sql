select
    /* Primary Key */
    plays.id
    /* Foreign Keys */
    , plays.game_id
    , plays.event_idx
    , plays.event_id
    , plays.player_id
    , plays.team_id
    /* Properties */
    , plays.player_role
    , plays.player_role_team
    , plays.event_type
    , plays.event_code
    , plays.event_description
    , plays.play_x_coordinate
    , plays.play_y_coordinate
    , plays.play_period
    , plays.play_period_type
    , plays.play_period_time_elapsed
    , plays.play_period_time_remaining
    , plays.play_period_seconds_elapsed
    , plays.play_period_seconds_remaining
    , plays.play_total_seconds_elapsed
    , plays.play_total_seconds_remaining
    , plays.play_time
    , plays.shots_away
    , plays.shots_home
    , plays.hits_away
    , plays.hits_home
    , plays.faceoffs_away
    , plays.faceoffs_home
    , plays.takeaways_away
    , plays.takeaways_home
    , plays.giveaways_away
    , plays.giveaways_home
    , plays.missedshots_away
    , plays.missedshots_home
    , plays.blockedshots_away
    , plays.blockedshots_home
    , plays.penalties_away
    , plays.penalties_home
    , plays.first_goal_scored
    , plays.last_goal_scored
    , plays.goals_away
    , plays.goals_home
    , plays.goal_difference_current
    , plays.winning_team_current
    , plays.game_state_current
    , plays.home_result_of_play
    , plays.away_result_of_play
    , plays.last_goal_game_winning
    , plays.last_goal_game_tying
    , plays.goals_home_lag
    , plays.goals_away_lag
    , plays.goal_difference_lag
    , plays.winning_team_lag
    , plays.game_state_lag
from
    {{ ref('stg_nhl__live_plays') }} as plays

