select
  /* Primary Key */
  stg_nhl__draft_id as draft_id

  /* Identifiers */
  , overall_pick_id
  , draft_prospect_id
  , draft_team_id

  /* Properties */
  , draft_year
  , draft_overall_pick
  , draft_round
  , draft_round_pick
  , draft_prospect_name
  , draft_url
  , draft_team_name
from {{ ref('stg_nhl__draft') }}
