select
    /* Primary Key */
    st.new_shift_id

    /* Identifiers */
    , st.game_id
    , st.player_id
    , st.team_id
    , concat(st.game_time_seconds, '_', st.start_seconds_elapsed) as game_time_id

    /* Shift Properties */
    , st.shift_number
    , st.is_shift_start
    , st.is_shift_end
    , st.start_time
    , st.end_time
    , st.duration

    /* Properties */
    , st.home_away_team
    , st.game_type_description
    , st.event_number
    , st.type_code
    , st.detail_code
    , st.player_full_name
    , st.is_goal
    , st.is_period_start
    , st.is_period_end
    , st.goal_game_state
    , st.goal_assisters
    , st.goal_primary_assister_full_name
    , st.goal_secondary_assister_full_name
    , st.period
    , st.period_type
    , st.game_time_seconds
    , st.shift_time_seconds
    , st.start_seconds_elapsed
    , st.end_seconds_elapsed
    , st.duration_seconds_elapsed
    , st.game_state
    , st.game_state_description
    , st.game_state_skaters
    , st.home_goalie_on_ice = 0 as home_goalie_pulled
    , st.away_goalie_on_ice = 0 as away_goalie_pulled
    , st.home_skaters
    , st.away_skaters
    , st.home_skaters_on_ice
    , st.away_skaters_on_ice
    , st.home_goalie_on_ice
    , st.home_defence_on_ice
    , st.home_forward_on_ice
    , st.away_goalie_on_ice
    , st.away_defence_on_ice
    , st.away_forward_on_ice
from {{ ref('stg_nhl__shifts_time') }} as st
