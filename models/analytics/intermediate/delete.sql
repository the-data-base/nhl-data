select 
  player.full_name
  ,
  , plays.* 
from `nhl-breakouts.dbt_dom.f_plays` as plays
left join `nhl-breakouts.dbt_dom.d_players` as player on player.player_id = plays.player_id
where 1 = 1
    --and plays.player_role in ("SCORER")
  and plays.event_type = 'GOAL'
  and plays.player_role = 'ASSIST'
  and plays.play_period_type <> 'SHOOTOUT'