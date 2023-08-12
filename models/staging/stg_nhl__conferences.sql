select
  /* Primary Key */
  {{ dbt_utils.surrogate_key(['conferences.id']) }} as stg_nhl__conferences_id

  /* Identifiers */
  , conferences.id as conference_id

  /* Properties */
  , conferences.name as conference_name
  , conferences.shortname as conference_short_name
  , conferences.abbreviation as conference_abbreviation
  , conferences.active as is_active
  , conferences.link as conference_url
from {{ source('meltano', 'conferences') }} as conferences
