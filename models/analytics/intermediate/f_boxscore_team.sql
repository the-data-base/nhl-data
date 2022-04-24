-- #TODO
select
    *
from
    {{ ref('stg_nhl__boxscore_team') }} as boxscore_team

order by
    boxscore_team.game_id desc
