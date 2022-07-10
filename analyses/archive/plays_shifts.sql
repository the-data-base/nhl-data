-- query #1: summarize missing shifts
with plays_shifts as (
select plays.*, plays.game_id as plays_game_id, shifts.*, schedule.schedule_id, schedule.game_type, schedule.game_type_description, season.season_id
from  `nhl-breakouts.dbt_dom.f_plays` as plays
left join `nhl-breakouts.dbt_dom.stg_nhl__shifts_time` as shifts on shifts.game_id = plays.game_id and shifts.game_time_seconds = plays.play_total_seconds_elapsed and shifts.player_id = plays.player_id
left join `nhl-breakouts.dbt_dom.d_schedule` as schedule on schedule.game_id = plays.game_id
left join `nhl-breakouts.dbt_dom.d_seasons` as season on season.season_id = schedule.season_id
)

select
  p.event_type
  , sum(case when p.new_shift_id is null then 1 else 0 end) as missing_shift
  , sum(case when p.new_shift_id is not null then 1 else 0 end) as has_shift
  , round((sum(case when p.new_shift_id is not null then 1 else 0 end) / count(*)), 4) as pcnt_has_shift
  , count(*) as ct
from plays_shifts as p
where
  1 = 1
  and p.new_shift_id is null
  and game_type in ('03', '02')
  and lower(play_period_type) <> 'shootout'
group by p.event_type
order by count(*) desc;

/**
-- gameids to investigate
2015020497 -- 487 missing plays-shifts (we are missing this game entirely)
2016021070 -- 102 missing plays-shifts (this one we are just missing a bunch... why?)
2019030016 -- 37 missing plays-shifts
2016020282 -- 33 missing plays-shifts
2019020259 -- 27 missing plays-shifts
2019020021 -- 26 missing plays-shifts
2018020732 -- 25 missing plays-shifts
2019020457 -- 24 missing plays-shifts
2019020169 -- 21 missing plays-shifts
2019020963 -- 19 missing plays-shifts
**/

-- query #2: drill into missing shifts
with plays_shifts as (
select plays.*, plays.game_id as plays_game_id, shifts.*, schedule.schedule_id, schedule.game_type, schedule.game_type_description, season.season_id
from  `nhl-breakouts.dbt_dom.f_plays` as plays
left join `nhl-breakouts.dbt_dom.stg_nhl__shifts_time` as shifts on shifts.game_id = plays.game_id and shifts.game_time_seconds = plays.play_total_seconds_elapsed and shifts.player_id = plays.player_id
left join `nhl-breakouts.dbt_dom.d_schedule` as schedule on schedule.game_id = plays.game_id
left join `nhl-breakouts.dbt_dom.d_seasons` as season on season.season_id = schedule.season_id
)

select *
from plays_shifts as p
where
    1 = 1
    and p.new_shift_id is null
    and game_type in ('03', '02')
    and lower(play_period_type) <> 'shootout'
    and plays_game_id = 2016021070

--query#3: drill into missing shifts
with plays_shifts as (
select
    plays.*
    -- shifts-time stuff
    ,shifts.new_shift_id
    ,shifts.shift_number
    ,shifts.is_shift_start
    ,shifts.is_shift_end
    ,shifts.start_time
    ,shifts.end_time
    ,shifts.duration
    ,shifts.shift_time_seconds
    ,shifts.start_seconds_elapsed
    ,shifts.end_seconds_elapsed
    ,shifts.duration_seconds_elapsed
    ,shifts.home_away_team
    ,shifts.game_type_description
    ,shifts.is_goal
    ,shifts.goal_game_state
    ,shifts.goal_assisters
    ,shifts.goal_primary_assister_full_name
    ,shifts.goal_secondary_assister_full_name
    ,shifts.period_type
    ,shifts.game_state
    ,shifts.game_state_description
    ,shifts.game_state_skaters
    ,shifts.home_goalie_pulled
    ,shifts.away_goalie_pulled
    ,shifts.home_skaters
    ,shifts.away_skaters
    ,shifts.home_skaters_on_ice
    ,shifts.away_skaters_on_ice
    ,shifts.home_goalie_on_ice
    ,shifts.home_defence_on_ice
    ,shifts.home_forward_on_ice
    ,shifts.away_goalie_on_ice
    ,shifts.away_defence_on_ice
    ,shifts.away_forward_on_ice
    -- schedule stuff
    ,schedule.schedule_id
    ,schedule.game_type
    ,schedule.game_type_description
    -- season stuff
    ,season.season_id
from  `nhl-breakouts.dbt_dom.f_plays` as plays
left join `nhl-breakouts.dbt_dom.stg_nhl__shifts_time` as shifts on shifts.game_id = plays.game_id and shifts.game_time_seconds = plays.play_total_seconds_elapsed and shifts.player_id = plays.player_id and shifts.is_shift_start is false
left join `nhl-breakouts.dbt_dom.d_schedule` as schedule on schedule.game_id = plays.game_id
left join `nhl-breakouts.dbt_dom.d_seasons` as season on season.season_id = schedule.season_id
)

select *
from plays_shifts as p
where
    1 = 1
    --and p.new_shift_id is null
    and game_type in ('03', '02')
    and lower(play_period_type) <> 'shootout'
    and game_id <> 2015020497
limit 10000
