SELECT
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['year', 'pickoverall']) }} as id

    /* Foreign Keys */
    ,CONCAT(CAST(draft.year as STRING), LPAD(CAST(draft.pickOverall as STRING), 3, '0')) AS overall_pick_id
    ,draft.prospect.id as draft_prospect_id
    ,draft.team.id as draft_team_id
    
    /* Properties */
    ,draft.year as draft_year
    ,draft.pickOverall as draft_overall_pick
    ,draft.round as draft_round
    ,draft.pickInRound as draft_round_pick
    ,draft.prospect.fullName as draft_prospect_name
    ,draft.prospect.link as draft_url
    ,draft.team.name as draft_team_name

FROM 
    {{ source('meltano', 'draft') }} as draft