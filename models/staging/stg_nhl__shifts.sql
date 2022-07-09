with shifts_raw as (
    select
        s.id as shift_id
        , s.shiftnumber as shift_number
        , s.gameid as game_id
        , s.playerid as player_id
        , s.teamid as team_id
        , case when s.teamid = schedule.home_team_id then 'home' when s.teamid = schedule.away_team_id then 'away' end as home_away_team
        , schedule.game_type
        , schedule.game_type_description
        , s.eventnumber as event_number
        , s.typecode as type_code
        , s.detailcode as detail_code
        , s.period
        , s.starttime as start_time
        , s.endtime as end_time
        , s.duration
        , case
            when s.starttime is not null
                 then cast( split(s.starttime, ':')[offset(0)] as int64)
        end as start_time_mins
        , case
            when s.starttime is not null
                 then cast( split(s.starttime, ':')[offset(1)] as int64)
        end as start_time_seconds
        , case
            when s.duration is not null
                 then cast( split(s.duration, ':')[offset(0)] as int64)
        end as duration_mins
        , case
            when s.duration is not null
                 then cast( split(s.duration, ':')[offset(1)] as int64)
        end as duration_seconds
        , concat(s.firstname, " ", s.lastname) as player_full_name
        , case
            when schedule.game_type = '02' and s.period = 4
                 then 'overtime'
            when schedule.game_type = '02' and s.period = 5
                 then 'shootout'
            when s.period > 3
                 then 'overtime'
            when s.period between 1 and 3
                 then 'regulation'
            else 'unknown'
        end as period_type
        , case
            when schedule.game_type = '02' and s.period = 5
                then false
            when s.typecode = 505
                then true
            else false
        end as is_goal
        , case
            when s.startTime = '00:00'
                then true
            else false
        end as is_period_start
        , case
            when s.endTime = '20:00'
                then true
            else false
        end as is_period_end
        , s.eventdescription as goal_game_state
        , s.eventdetails as goal_assisters
        , case
            when s.typecode <> 505
                 then 'not a goal'
            when s.eventdetails is null
                 then 'unassisted'
            when length(s.eventdetails) - length(replace(s.eventdetails, ',', '')) > 0
                                                         then '2 assisters'
            else '1 assister'
        end as goal_assist_count
    from {{ source('meltano', 'shifts') }} as s
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = s.gameid
    where 1 = 1
        and s.playerid is not null
        -- manually removing shift ids of goals that were duplicated with different values (either in time of goal, or eventdetails)
        -- method: cross referenced the gameid with the nhl boxscore, manually removed the incorrect line-item
        -- TODO: investigate if we can cross validate with boxscore data
        and not (s.gameid = 2020020279 and s.eventnumber = 820 and s.id = 10501471) -- eventdetails were off
        and not (s.gameid = 2020020038 and s.eventnumber = 668 and s.id = 10335336) -- time of goal was off
        and not (s.gameid = 2020020249 and s.eventnumber = 61 and s.id = 10481186) -- time of goal was off
        and not (s.gameid = 2020020274 and s.eventnumber = 472 and s.id = 10498099) -- time of goal and eventdetails were off
        and not (s.gameid = 2020020129 and s.eventnumber = 630 and s.id = 10408031) -- time of goal was way off
        and not (s.gameid = 2020030113 and s.eventnumber = 57 and s.id = 10995042) -- time of goal was off
        and not (s.gameid = 2020020056 and s.eventnumber = 538 and s.id = 10349144) -- eventdetails were off
        and not (s.gameid = 2020020792 and s.eventnumber = 220 and s.id = 10900308) -- time of goal was off
        and not (s.gameid = 2020020279 and s.eventnumber = 566 and s.id = 10501469) -- time of goal was off
        and not (s.gameid = 2020020003 and s.eventnumber = 222 and s.id = 10310433) -- time of goal was off
        and not (s.gameid = 2020020001 and s.eventnumber = 70 and s.id = 10309429) -- time of goal was off
        and not (s.gameid = 2020030412 and s.eventnumber = 614 and s.id = 11050284) -- time of goal was off
        and not (s.gameid = 2020020745 and s.eventnumber = 485 and s.id = 10875539) -- time of goal was off
        and not (s.gameid = 2020020767 and s.eventnumber = 209 and s.id = 10639438) -- time of goal was off
)

, shifts_time as (
    select
        case
            when period = 1
                then (shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds)
            when period = 2
                then (20 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 3
                then (40 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 4
                then (60 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 5
                then (80 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 6
                then (100 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 7
                then (120 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 8
                then (140 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 9
                then (160 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
            when period = 10
                then (180 * 60) + ((shifts_raw.start_time_mins * 60) + (shifts_raw.start_time_seconds))
        end as start_seconds_elapsed
        , case
            when shifts_raw.type_code = 505
                then 0
            else (shifts_raw.duration_mins * 60) + (shifts_raw.duration_seconds)
        end as duration_seconds_elapsed
        , case
            when shifts_raw.goal_assist_count = '2 assisters'
                 then trim(split(shifts_raw.goal_assisters, ',')[offset(0)])
            when shifts_raw.goal_assist_count = '1 assister'
                 then shifts_raw.goal_assisters
        end as goal_primary_assister_full_name
        , case
            when shifts_raw.goal_assist_count = '2 assisters'
                 then trim(split(shifts_raw.goal_assisters, ', ')[offset(1)])
        end as goal_secondary_assister_full_name
        , shifts_raw.*
    from shifts_raw
    where shifts_raw.period_type <> 'shootout'
)

select
    shifts_time.shift_id
    , shifts_time.shift_number
    , shifts_time.game_id
    , shifts_time.player_id
    , shifts_time.team_id
    , shifts_time.home_away_team
    , shifts_time.game_type_description
    , shifts_time.event_number
    , shifts_time.type_code
    , shifts_time.detail_code
    , shifts_time.player_full_name
    , shifts_time.is_goal
    , shifts_time.is_period_start
    , shifts_time.is_period_end
    , shifts_time.goal_game_state
    , shifts_time.goal_assisters
    , shifts_time.goal_primary_assister_full_name
    , shifts_time.goal_secondary_assister_full_name
    , shifts_time.period
    , shifts_time.period_type
    , shifts_time.start_seconds_elapsed
    , (shifts_time.start_seconds_elapsed + shifts_time.duration_seconds_elapsed) as end_seconds_elapsed
    , shifts_time.duration_seconds_elapsed
    , shifts_time.start_time
    , shifts_time.end_time
    , shifts_time.duration
from shifts_time
