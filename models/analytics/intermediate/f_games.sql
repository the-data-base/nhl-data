-- #TODO
select
    *

from
    {{ ref('stg_meltano__games') }} AS boxscore_game

order by
    boxscore_game.id desc
