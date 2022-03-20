-- #TODO
select  
   *

from     
    {{ ref('stg_meltano__boxscore_player') }} as boxscore_player

order by
    boxscore_player.game_id desc
    , boxscore_player.team_id desc
    , boxscore_player.player_id  desc