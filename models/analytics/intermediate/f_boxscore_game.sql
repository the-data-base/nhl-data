-- #TODO
select  
    *

from     
    {{ ref('stg_meltano__boxscore_game') }} AS boxscore_game

order by
    boxscore_game.id desc