with game_seconds as (
  select
    -1 + row_number() over() as seconds
  from unnest((select split(format("%10800s", ""),'') as h from (select null))) as pos
  order by seconds
)

, seconds_between_shifts as (
  select
    concat(shifts.shift_id, '_', gs.seconds) as new_shift_id
    ,gs.seconds as game_time_seconds
    ,shifts.*
  from `nhl-breakouts.dbt_dom.stg_nhl__shifts` as shifts
  inner join game_seconds as gs on gs.seconds between shifts.start_seconds_elapsed and shifts.end_seconds_elapsed
  where shift_id = 6020337
)

select
    sbs.new_shift_id
    , sbs.shift_number
    , sbs.game_id
    , sbs.player_id
    , sbs.team_id
    , sbs.game_type_description
    , sbs.event_number
    , sbs.type_code
    , sbs.detail_code
    , sbs.player_full_name
    , sbs.goal_ind
    , sbs.goal_game_state
    , sbs.goal_assisters
    , sbs.goal_primary_assister_full_name
    , sbs.goal_secondary_assister_full_name
    , sbs.period
    , sbs.period_type
    , sbs.game_time_seconds
    , sbs.start_seconds_elapsed
    , sbs.end_seconds_elapsed
    , sbs.duration_seconds_elapsed
    , sbs.start_time
    , sbs.end_time
    , sbs.duration
from
    seconds_between_shifts as sbs
order by sbs.game_time_seconds
limit 100


players_on_ice
{
    home: [{}],
    away: [{}]
}}
