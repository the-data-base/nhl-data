select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['year', 'pickoverall']) }} as id

    /* Foreign Keys */
    , team.id as team_id
    , prospect.id as prospect_id

    /* Properties */
    , year as draft_year
    , round
    , pickOverall as pick_overall
    , pickInRound as pick_in_round
    , team.name as team_name
    , team.link as team_url
    , prospect.fullName as prospect_full_name
    , prospect.link as prospect_url
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'draft') }}
