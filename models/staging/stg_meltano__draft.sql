SELECT
    /* Primary Key */
    --{{ dbt_utils.surrogate_key(['year', 'pickoverall']) }} as id
    CONCAT(CAST(d.year as STRING), LPAD(CAST(d.pickOverall as STRING), 3, '0')) AS id

    /* Foreign Keys */
    ,d.prospect.id as draft_prospect_id
    ,d.team.id as draft_team_id
    
    /* Properties */
    ,d.year as draft_year
    ,d.pickOverall as draft_overall_pick
    ,d.round as draft_round
    ,d.pickInRound as draft_round_pick
    ,d.prospect.fullName as draft_prospect_name
    ,d.prospect.link as draft_url
    ,d.team.name as draft_team_name

FROM 
from {{ source('meltano', 'draft') }}
