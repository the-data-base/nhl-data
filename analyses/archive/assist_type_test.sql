with test as (
  select 
    player.full_name as pfn
    , REGEXP_SUBSTR(plays.event_description, ": (.*)") as assist_description
    , schedule.game_type
    , plays.* 
  from `nhl-breakouts.dbt_dom.f_plays` as plays
  left join `nhl-breakouts.dbt_dom.d_players` as player on player.player_id = plays.player_id
  left join`nhl-breakouts.dbt_dom.d_schedule` as schedule on schedule.game_id = plays.game_id
  left join `nhl-breakouts.dbt_dom.d_seasons` as season on season.season_id = schedule.season_id
  where 1 = 1
    and schedule.game_type = '02'
    and plays.event_type = 'GOAL'
    and plays.player_role = 'ASSIST'
    and plays.play_period_type <> 'SHOOTOUT'
)

,test2 as (
select 
  test.assist_description
  , TRIM(REGEXP_SUBSTR(test.assist_description, "[^()]+")) as assist_primary_player_full_name
  , TRIM(REGEXP_SUBSTR ( REGEXP_SUBSTR(test.assist_description, ", (.*)"), "[^()]+")) as assist_secondary_player_full_name
  , test.*
from test
)

,test3 as (
select 
  test2.pfn
  ,case when test2.pfn = test2.assist_primary_player_full_name then 1 else 0 end as assist_primary
  ,case when test2.pfn = test2.assist_secondary_player_full_name then 1 else 0 end as assist_secondary
  ,test2.*
from test2
)

select test3.* 
from test3
where (assist_primary + assist_secondary) = 0


-- problem: only way to get primary or secondary assist is from the event_description... which we can do. But, if the player's name is spelt incorrectly or nicknamed, there is no way to tie it back as there is no "nickame" at the row level, and the join back to d_players will have a diff name
-- solution: fuzzy join? mapping table? include the name from stg_nhl__live_plays and use this in the case when?
;
