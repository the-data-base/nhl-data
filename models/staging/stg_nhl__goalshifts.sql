with

goal_shifts as (
  select 
    s.id as shift_id
    ,s.playerId as player_id
    ,s.gameId as game_id
    ,s.teamId as team_id
    ,s.period
    ,s.startTime as start_time
    ,s.typeCode as type_code
    ,s.detailCode as detail_code
    ,s.eventNumber as event_number
    ,s.eventDescription as game_state
    ,CONCAT(s.firstName, " ", s.lastName) as player_full_name
    ,s.eventDetails as assist_player_description
    ,case 
      when s.eventDetails is null then 'Unassisted'
      when LENGTH(s.eventDetails) - LENGTH(REPLACE(s.eventDetails, ',', '')) > 0 then '2 Assisters'
      else '1 Assister'
      end as assist_count
    ,s._time_extracted
    ,s._time_loaded
  from {{ source('meltano', 'shifts') }} as s
  where
    1 = 1
    and s.typeCode = 505 -- goals
)

 select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['shifts.shift_id']) }} as stg_nhl__shift_id
    
    /* Identifiers */
    ,g.shift_id
    ,g.player_id
    ,g.game_id
    ,g.team_id
    ,g.type_code
    ,g.detail_code
    ,g.event_number
    
    /* Properties */
    ,g.period
    ,g.start_time
    ,g.game_state
    ,g.player_full_name
    ,g.assist_player_description
    ,case 
    when g.assist_count = '2 Assisters' then trim(split(g.assist_player_description, ',')[offset(0)])
    when g.assist_count = '1 Assister' then g.assist_player_description
    else 'None'
    end as assist_primary_player_full_name
    ,case 
    when g.assist_count = '2 Assisters' then trim(split(g.assist_player_description, ', ')[offset(1)])
    else 'None'
    end as assist_secondary_player_full_name 
    ,g.assist_count
    ,g._time_extracted
    ,g._time_loaded
from goal_shifts as g