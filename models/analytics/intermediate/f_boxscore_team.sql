-- #TODO
select  
    *
from     
    {{ ref('stg_meltano__boxscore_team') }} as boxscore_team

order by
    boxscore_team.game_id desc