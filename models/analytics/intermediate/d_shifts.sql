with

shifts as (
    select
        /* Primary Key */
        shifts.shift_id

        /* Foreign Keys */
        , shifts.game_id
        , shifts.player_id
        , shifts.team_id

        /* Properties */
        , shifts.shift_number
        , case
            when shifts.team_id = schedule.home_team_id then 'Home'
            when shifts.team_id = schedule.away_team_id then 'Away'
            else 'Unknown'
        end as home_away_team
        , schedule.game_type
        , schedule.game_type_description
        , shifts.detail_code
        , shifts.event_number
        , shifts.goal_game_state
        , shifts.period
        , shifts.player_full_name
        , shifts.type_code

        /* Shift Times */
        , shifts.start_time
        , shifts.start_time_mins
        , shifts.start_time_seconds
        , shifts.end_time
        , shifts.duration
        , shifts.duration_mins
        , shifts.duration_seconds
        , case
            when shifts.period = 1 then (shifts.start_time_mins * 60) + (shifts.start_time_seconds)
            when shifts.period = 2 then (20 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 3 then (40 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 4 then (60 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 5 then (80 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 6 then (100 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 7 then (120 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 8 then (140 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 9 then (160 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
            when shifts.period = 10 then (180 * 60) + ((shifts.start_time_mins * 60) + (shifts.start_time_seconds))
        end as start_seconds_elapsed
        , if(shifts.type_code = 505, 0, (shifts.duration_mins * 60) + (shifts.duration_seconds)) as duration_seconds_elapsed

        /* Assister Properties */
        , shifts.goal_assisters
        , shifts.goal_assist_count
        , case
            when shifts.goal_assist_count = '2 assisters' then trim(split(shifts.goal_assisters, ',')[offset(0)])
            when shifts.goal_assist_count = '1 assister' then shifts.goal_assisters
        end as goal_primary_assister_full_name
        , case
            when shifts.goal_assist_count = '2 assisters' then trim(split(shifts.goal_assisters, ', ')[offset(1)])
        end as goal_secondary_assister_full_name
        , case
            when schedule.game_type = '02' and shifts.period = 4 then 'overtime'
            when schedule.game_type = '02' and shifts.period = 5 then 'shootout'
            when shifts.period > 3 then 'overtime'
            when shifts.period between 1 and 3 then 'regulation'
            else 'unknown'
        end as period_type

        /* Flags */
        , case
            when schedule.game_type = '02' and shifts.period = 5 then false
            when shifts.type_code = 505 then true
            else false
        end as is_goal
        , shifts.is_period_start
        , shifts.is_period_end
    from {{ ref('stg_nhl__shifts') }} as shifts
    left join {{ ref('d_schedule') }} as schedule
        on shifts.game_id = schedule.game_id
    where 1 = 1
        and not (schedule.game_type = '02' and shifts.period = 5) -- remove shootouts
        and shifts.end_time != '' -- remove 1129 shifts, dups
)


, find_duplicate_shifts as (
    -- as of 7/10/2022, there were 91 shifts that mapped to 13 plays where the shift was duplicated having the same start time but different durations (thus, different end times)
    -- rule: if a player's shift is duplicated, take the shift that was longer since they might have been involved in the play (min takes shorter duration, max takes longer)
    select
        shift_id
        , game_id
        , player_id
        , period
        , start_seconds_elapsed
        , event_number
        , shift_number
        , row_number() over(
            partition by
                game_id
                , player_id
                , period
                , start_seconds_elapsed
            order by
                period asc
                , start_seconds_elapsed asc
                , duration desc
        ) as duplicate_sequence_number -- 1 = shift among duplicates with the longest duration, >1 means duplicates we don't care about
    from shifts
    where not is_goal
)

, deduped_shift_attributes as (
    -- return the original shift and event numbers that were identified as deduplicates as an array
    select
        game_id
        , player_id
        , period
        , start_seconds_elapsed
        , string_agg(cast(shift_number as string)) as shift_numbers
        , string_agg(cast(event_number as string)) as event_numbers
        , string_agg(cast(shift_id as string)) as shift_ids
    from find_duplicate_shifts
    group by 1, 2, 3, 4
)

, revised_shift_number_for_duplicate_shifts as (
    -- create the new_shift_number, excluding any shifts that were duplicated as well as goals
    select
        shift_id
        , game_id
        , player_id
        , start_seconds_elapsed
        , period
        , shift_number as original_shift_number
        , row_number() over(
            partition by
                game_id
                , player_id
            order by
                start_seconds_elapsed asc
        ) as revised_shift_number
    from find_duplicate_shifts
    where
        duplicate_sequence_number = 1
)

select
    shifts.shift_id
    , shifts.game_id
    , shifts.player_id
    , shifts.team_id
    , if(shifts.is_goal, shifts.shift_number, revised_shift_number_for_duplicate_shifts.revised_shift_number) as shift_number
    , deduped_shift_attributes.shift_numbers
    , deduped_shift_attributes.shift_ids
    , deduped_shift_attributes.event_numbers
    , shifts.start_time
    , shifts.end_time
    , shifts.duration
    , shifts.duration_seconds_elapsed
    , shifts.start_seconds_elapsed
    , (shifts.start_seconds_elapsed + shifts.duration_seconds_elapsed) as end_seconds_elapsed
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
from shifts
left join revised_shift_number_for_duplicate_shifts
    on shifts.shift_id = revised_shift_number_for_duplicate_shifts.shift_id
left join deduped_shift_attributes
    on shifts.game_id = deduped_shift_attributes.game_id
        and shifts.player_id = deduped_shift_attributes.player_id
        and shifts.period = deduped_shift_attributes.period
        and shifts.start_seconds_elapsed = deduped_shift_attributes.start_seconds_elapsed
where
    shifts.is_goal -- include goals
    or revised_shift_number_for_duplicate_shifts.shift_id is not null -- exclude duplicates from prior steps
