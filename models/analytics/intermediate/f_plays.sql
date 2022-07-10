select
    /* Primary Key */
    plays.stg_nhl__live_plays_id as play_id

    /* Identifiers */
    , plays.game_id
    , plays.event_idx
    , plays.event_id
    , plays.player_id
    , plays.team_id

    /* Play properties */
    , plays.player_full_name
    , plays.player_primary_assist
    , plays.player_secondary_assist
    , plays.player_role
    , plays.player_role_team
    , plays.event_type
    , plays.event_code
    , plays.event_description
    , plays.event_secondary_type
    , plays.penalty_severity
    , plays.penalty_minutes
    , plays.play_x_coordinate
    , plays.play_y_coordinate
    , plays.play_total_seconds_elapsed
    , plays.play_period
    , plays.play_period_type
    , plays.play_period_time_elapsed
    , plays.play_period_time_remaining
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

    /* Shift properties */
    , shifts.new_shift_id
    , shifts.new_shift_number
    , shifts.shift_numbers
    , shifts.shift_ids
    , shifts.event_numbers as shift_event_numbers
    , shifts.start_time as shift_start_time
    , shifts.end_time as shift_end_time
    , shifts.duration as shift_duration
    , shifts.goal_game_state
    , shifts.is_shift_start
    , shifts.is_shift_end
    , shifts.is_shift_start_period_start
    , shifts.is_shift_start_not_period_start
    , shifts.is_shift_end_period_end
    , shifts.game_state
    , shifts.game_state_description
    , shifts.game_state_skaters
    , shifts.home_goalie_on_ice = 0 as home_goalie_pulled
    , shifts.away_goalie_on_ice = 0 as away_goalie_pulled
    , shifts.home_skaters
    , shifts.away_skaters
    , shifts.home_skaters_on_ice
    , shifts.away_skaters_on_ice
    , shifts.home_defence_on_ice
    , shifts.away_defence_on_ice
    , shifts.home_forward_on_ice
    , shifts.away_forward_on_ice
    , shifts.home_goalie_on_ice
    , shifts.away_goalie_on_ice

from {{ ref('stg_nhl__live_plays') }} as plays
left join {{ ref('stg_nhl__shifts_time') }} as shifts
    on shifts.game_id = plays.game_id
        and shifts.game_time_seconds = plays.play_total_seconds_elapsed
        and shifts.player_id = plays.player_id
        and shifts.period = plays.play_period
        and shifts.is_goal is false
