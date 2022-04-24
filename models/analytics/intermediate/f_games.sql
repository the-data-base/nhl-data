-- #TODO
select
    *

from
    {{ ref('stg_nhl__games') }} AS boxscore_game

order by
    boxscore_game.id desc
