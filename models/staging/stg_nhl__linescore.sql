with
    live_linescore as (
        select * from {{ source('meltano', 'live_linescore') }}
    )

    , final as (
        select
            /* Primary Key */
            game_id as id

            /* Foreign Keys */
            , game_id
            , teams.home.team.id as home_team_id
            , teams.away.team.id as away_team_id
            , case
                when teams.home.goals  > teams.away.goals then teams.home.team.id
                when teams.home.goals  < teams.away.goals then teams.away.team.id
                else -1
              end as game_winning_team_id

            /* Properties */
            -- Game-level stats
            , concat(teams.home.goals, '-', teams.away.goals, ' (Home-Away)') as game_score_description
            , concat(teams.home.team.name, ' (Home) vs ', teams.away.team.name, ' (Away)') as game_matchup_description
            , case
                when teams.home.goals  > teams.away.goals  then teams.home.team.name
                when teams.home.goals  < teams.away.goals  then teams.away.team.name
                else 'Undetermined'
              end as game_winning_team_name
            , case
                when teams.home.goals  > teams.away.goals  then 'Home'
                when teams.home.goals < teams.away.goals  then 'Away'
                else 'Undetermined'
              end as game_winning_team_type
            , abs(teams.home.goals - teams.away.goals) as game_absolute_goal_differential
            -- Team level stats
            , teams.home.goals as home_team_goals
            , teams.away.goals as away_team_goals
        from live_linescore
    )

select * from final
