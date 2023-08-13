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
    , plays.event_code
    , plays.event_type
    , plays.event_secondary_type
    , plays.event_description
    , plays.last_player_role_team
    , plays.last_play_event_type
    , plays.last_play_event_secondary_type
    , plays.last_play_event_description
    , plays.last_play_period
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
    -- seconds since last shot: if the last shot was taken by the same team in the same period, get the time elapsed between shots
    , case
        when plays.last_shot_saved_shot_ind = 1
            then (plays.play_total_seconds_elapsed - plays.last_shot_total_seconds_elapsed)
        else 0
    end as seconds_since_last_shot
    -- rebounds: if the last shot was take by the same team in the same period, and the time elapsed between shots was between 0 - 2 seconds, then 1 else 0
    , case
        when
            plays.last_shot_saved_shot_ind = 1
            and (plays.play_total_seconds_elapsed - plays.last_shot_total_seconds_elapsed) <= 2
            then 1
        else 0
    end as shot_rebound_ind
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

    /* Location properties */
    , loc.adj_play_x_coordinate as adj_x_coordinate
    , loc.adj_play_y_coordinate as adj_y_coordinate
    , loc.play_distance
    , loc.play_angle
    , loc.rink_side
    , loc.zone_type
    , loc.zone

    /* Last shot location properties */
    , plays.last_shot_event_idx
    , plays.last_shot_team_id
    , plays.last_shot_period
    , plays.last_shot_total_seconds_elapsed
    , plays.last_shot_event_type
    , plays.last_shot_event_secondary_type
    , plays.last_shot_x_coordinate
    , plays.last_shot_y_coordinate
    , plays.last_shot_saved_shot_ind
    -- seconds since last shot: if the last shot was take by the same team in the same period, get the time elapsed between shots
    , case
        when plays.last_shot_saved_shot_ind = 1
            then (plays.play_total_seconds_elapsed - plays.last_shot_total_seconds_elapsed)
        else 0
    end as last_shot_seconds
    -- rebounds: if the last shot was take by the same team in the same period, and the time elapsed between shots was between 0 - 2 seconds, then 1 else 0
    , case
        when
            plays.last_shot_saved_shot_ind = 1
            and plays.last_play_period = plays.play_period
            and lower(plays.last_play_event_type) in ('blocked_shot', 'missed_shot', 'shot', 'goal')
            and (plays.play_total_seconds_elapsed - plays.last_shot_total_seconds_elapsed) <= 2
            then 1
        else 0
    end as last_shot_rebound_ind

    /* Last play location properties */
    , lag(loc.adj_play_x_coordinate) over (partition by plays.game_id order by plays.event_idx) as last_play_adj_x_coordinate
    , lag(loc.adj_play_y_coordinate) over (partition by plays.game_id order by plays.event_idx) as last_play_adj_y_coordinate
    , lag(loc.play_distance) over (partition by plays.game_id order by plays.event_idx) as last_play_distance
    , lag(loc.play_angle) over (partition by plays.game_id order by plays.event_idx) as last_play_angle
    , lag(loc.rink_side) over (partition by plays.game_id order by plays.event_idx) as last_play_rink_side
    , lag(loc.zone_type) over (partition by plays.game_id order by plays.event_idx) as last_play_zone_type
    , lag(loc.zone) over (partition by plays.game_id order by plays.event_idx) as last_play_zone

    /* Shift properties */
    , shifts.shift_id
    , shifts.shift_number
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

    /* XG stuff */
    , coalesce(xg.id_fenwick_shot, 0) as xg_fenwick_shot
    , xg.x_goal
    , xg.xg_model_id
    , xg.id_strength_state_code as xg_strength_state_code
    , xg.xg_proba

from {{ ref('stg_nhl__live_plays') }} as plays
left join {{ ref('d_shifts_time') }} as shifts
    on
        shifts.game_id = plays.game_id
        and shifts.game_time_seconds = plays.play_total_seconds_elapsed
        and shifts.player_id = plays.player_id
        and shifts.period = plays.play_period
        and shifts.is_goal is false
left join {{ ref('stg_nhl__live_plays_location') }} as loc
    on
        loc.play_id = plays.stg_nhl__live_plays_id
        and loc.game_id = plays.game_id
        and loc.play_x_coordinate = cast(plays.play_x_coordinate as float64)
        and loc.play_y_coordinate = cast(plays.play_y_coordinate as float64)
left join {{ ref('stg_nhl__xg') }} as xg
    on
        xg.id_play_id = plays.stg_nhl__live_plays_id
        and xg.id_game_id = plays.game_id
        and xg.id_player_id = plays.player_id
