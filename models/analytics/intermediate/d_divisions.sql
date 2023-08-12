select
  /* Primary Key */
  division_id

  /* Identifiers */
  , conference_id

  /* Properties */
  , division_name
  , division_short_name
  , division_url
  , division_abbreviation
  , is_active

from {{ ref('stg_nhl__divisions') }}
