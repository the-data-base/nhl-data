select
    /* Primary Key */
    draft.id
    /* Foreign Keys */
    , draft.overall_pick_id
    , draft.draft_prospect_id
    , draft.draft_team_id
    /* Properties */
    , draft.draft_year
    , draft.draft_overall_pick
    , draft.draft_round
    , draft.draft_round_pick
    , draft.draft_prospect_name
    , draft.draft_url
    , draft.draft_team_name
from 
    {{ ref('stg_nhl__draft') }} as draft
