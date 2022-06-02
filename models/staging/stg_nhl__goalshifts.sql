with

goal_shifts as (
    select distinct
        CONCAT(s.gameid, '_', s.playerid, '_', s.eventnumber) as goalshift_id
        , s.eventnumber as event_number
        , s.playerid as player_id
        , s.gameid as game_id
        , s.teamid as team_id
        , s.period
        , s.starttime as start_time
        , s.typecode as type_code
        , s.detailcode as detail_code
        , s.eventdescription as game_state
        , CONCAT(s.firstname, " ", s.lastname) as player_full_name
        , s.eventdetails as assist_player_description
        , case
            when s.eventdetails is null then 'Unassisted'
            when LENGTH(s.eventdetails) - LENGTH(REPLACE(s.eventdetails, ',', '')) > 0 then '2 Assisters'
            else '1 Assister'
        end as assist_count
    from {{ source('meltano', 'shifts') }} as s

    where
        1 = 1
        and s.typecode = 505 -- goals
        and s.playerid is not null
        -- Manually removing shift IDs of goals that were duplicated with different values (either in time of goal, or eventDetails)
        -- Method: cross referenced the gameId with the NHL boxscore, manually removed the incorrect line-item
        and not (s.gameid = 2020020279 and s.eventnumber = 820 and s.id = 10501471) -- eventDetails were off
        and not (s.gameid = 2020020038 and s.eventnumber = 668 and s.id = 10335336) -- time of goal was off
        and not (s.gameid = 2020020249 and s.eventnumber = 61 and s.id = 10481186) -- time of goal was off
        and not (s.gameid = 2020020274 and s.eventnumber = 472 and s.id = 10498099) -- time of goal and eventDetails were off
        and not (s.gameid = 2020020129 and s.eventnumber = 630 and s.id = 10408031) -- time of goal was way off
        and not (s.gameid = 2020030113 and s.eventnumber = 57 and s.id = 10995042) -- time of goal was off
        and not (s.gameid = 2020020056 and s.eventnumber = 538 and s.id = 10349144) -- eventDetails were off
        and not (s.gameid = 2020020792 and s.eventnumber = 220 and s.id = 10900308) -- time of goal was off
        and not (s.gameid = 2020020279 and s.eventnumber = 566 and s.id = 10501469) -- time of goal was off
        and not (s.gameid = 2020020003 and s.eventnumber = 222 and s.id = 10310433) -- time of goal was off
        and not (s.gameid = 2020020001 and s.eventnumber = 70 and s.id = 10309429) -- time of goal was off
        and not (s.gameid = 2020030412 and s.eventnumber = 614 and s.id = 11050284) -- time of goal was off
        and not (s.gameid = 2020020745 and s.eventnumber = 485 and s.id = 10875539) -- time of goal was off
        and not (s.gameid = 2020020767 and s.eventnumber = 209 and s.id = 10639438) -- time of goal was off
)

select distinct
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['g.goalshift_id']) }} as stg_nhl__shift_id

    /* Identifiers */
    , g.player_id
    , g.game_id
    , g.team_id
    , g.type_code
    , g.detail_code
    , g.event_number

    /* Properties */
    , g.period
    , g.start_time
    , g.game_state
    , g.player_full_name
    , g.assist_player_description
    , case
        when g.assist_count = '2 Assisters' then TRIM(SPLIT(g.assist_player_description, ',')[OFFSET(0)])
        when g.assist_count = '1 Assister' then g.assist_player_description
        else 'None'
    end as assist_primary_player_full_name
    , case
        when g.assist_count = '2 Assisters' then TRIM(SPLIT(g.assist_player_description, ', ')[OFFSET(1)])
        else 'None'
    end as assist_secondary_player_full_name
    , g.assist_count
from goal_shifts as g