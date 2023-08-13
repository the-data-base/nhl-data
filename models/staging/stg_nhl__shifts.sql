select
    /* Primary Key */
    id as shift_id

    /* Foreign Keys */
    , gameid as game_id
    , teamid as team_id
    , playerid as player_id

    /* Properties */
    , detailcode as detail_code
    , duration
    , endtime as end_time
    , eventnumber as event_number
    , eventdetails as goal_assisters
    , eventdescription as goal_game_state
    , case
        when typecode != 505 then 'not a goal'
        when eventdetails is null then 'unassisted'
        when length(eventdetails) - length(replace(eventdetails, ',', '')) > 0 then '2 assisters'
        else '1 assister'
    end as goal_assist_count
    , period
    , concat(firstname, ' ', lastname) as player_full_name
    , shiftnumber as shift_number
    , starttime as start_time
    , typecode as type_code

    /* Shift time attributes */
    , if(starttime is not null, cast(split(starttime, ':')[offset(0)] as int64), null) as start_time_mins
    , if(starttime is not null, cast(split(starttime, ':')[offset(1)] as int64), null) as start_time_seconds
    , if(starttime is not null, cast(split(duration, ':')[offset(0)] as int64), null) as duration_mins
    , if(starttime is not null, cast(split(duration, ':')[offset(1)] as int64), null) as duration_seconds

    /* Flags */
    , starttime = '00:00' as is_period_start
    , endtime = '20:00' as is_period_end

from {{ source('meltano', 'shifts') }}
where
    playerid is not null
    /* manually removing shift ids of goals that were duplicated with different values (either in time of goal, or eventdetails)
    method: cross referenced the gameid with the nhl boxscore, manually removed the incorrect line-item
    TODO: investigate if we can cross validate with boxscore data */
    and not (gameid = 2020020279 and eventnumber = 820 and id = 10501471) -- eventdetails were off
    and not (gameid = 2020020038 and eventnumber = 668 and id = 10335336) -- time of goal was off
    and not (gameid = 2020020249 and eventnumber = 61 and id = 10481186) -- time of goal was off
    and not (gameid = 2020020274 and eventnumber = 472 and id = 10498099) -- time of goal and eventdetails were off
    and not (gameid = 2020020129 and eventnumber = 630 and id = 10408031) -- time of goal was way off
    and not (gameid = 2020030113 and eventnumber = 57 and id = 10995042) -- time of goal was off
    and not (gameid = 2020020056 and eventnumber = 538 and id = 10349144) -- eventdetails were off
    and not (gameid = 2020020792 and eventnumber = 220 and id = 10900308) -- time of goal was off
    and not (gameid = 2020020279 and eventnumber = 566 and id = 10501469) -- time of goal was off
    and not (gameid = 2020020003 and eventnumber = 222 and id = 10310433) -- time of goal was off
    and not (gameid = 2020020001 and eventnumber = 70 and id = 10309429) -- time of goal was off
    and not (gameid = 2020030412 and eventnumber = 614 and id = 11050284) -- time of goal was off
    and not (gameid = 2020020745 and eventnumber = 485 and id = 10875539) -- time of goal was off
    and not (gameid = 2020020767 and eventnumber = 209 and id = 10639438) -- time of goal was off
