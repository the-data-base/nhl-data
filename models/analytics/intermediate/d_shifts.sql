select
    /* Primary Key */
    shifts.new_shift_id

    /* Identifiers */
    , shifts.game_id
    , shifts.player_id
    , shifts.team_id

    /* Shift Properties */
    , shifts.new_shift_number
    , shifts.shift_ids
    , shifts.shift_numbers
    , shifts.event_numbers
    , shifts.start_time
    , shifts.end_time
    , shifts.duration
    , shifts.duration_seconds_elapsed
    , shifts.start_seconds_elapsed
    , shifts.end_seconds_elapsed

    /* Properties */
    , shifts.period
    , shifts.period_type
    , shifts.home_away_team
    , shifts.game_type_description
    , shifts.type_code
    , shifts.detail_code
    , shifts.player_full_name
    , shifts.is_goal
    , shifts.is_period_start
    , shifts.is_period_end
    , shifts.goal_game_state
    , shifts.goal_assisters
    , shifts.goal_primary_assister_full_name
    , shifts.goal_secondary_assister_full_name
from {{ ref('stg_nhl__shifts') }} as shifts
