with
    live_boxscore as (
        select * from {{ source('meltano', 'live_boxscore') }}
    )

    , final as (
        select
            /* Primary Key */
            game_id as id

            /* Foreign Keys */
            , game_id
            , teams.home.team.id as home_team_id
            , teams.away.team.id as away_team_id

            /* Properties */
            -- Home team stats
            , teams.home.team.name as home_team_name
            , teams.home.teamStats.teamSkaterStats.pim as home_team_pim
            , teams.home.teamStats.teamSkaterStats.shots as home_team_shots
            , teams.home.teamStats.teamSkaterStats.powerPlayGoals as home_team_powerplay_goals
            , teams.home.teamStats.teamSkaterStats.powerPlayOpportunities as home_team_powerplay_opportunities
            , teams.home.teamStats.teamSkaterStats.faceOffWinPercentage as home_team_faceoff_percentage
            , teams.home.teamStats.teamSkaterStats.blocked as home_team_blocked
            , teams.home.teamStats.teamSkaterStats.takeaways as home_team_takeaways
            , teams.home.teamStats.teamSkaterStats.giveaways as home_team_giveaways
            , teams.home.teamStats.teamSkaterStats.hits as home_team_hits

            -- Away team stats
            , teams.away.team.name as away_team_name
            , teams.away.teamStats.teamSkaterStats.pim as away_team_pim
            , teams.away.teamStats.teamSkaterStats.shots as away_team_shots
            , teams.away.teamStats.teamSkaterStats.powerPlayGoals as away_team_powerplay_goals
            , teams.away.teamStats.teamSkaterStats.powerPlayOpportunities as away_team_powerplay_opportunities
            , teams.away.teamStats.teamSkaterStats.faceOffWinPercentage as away_team_faceoff_percentage
            , teams.away.teamStats.teamSkaterStats.blocked as away_team_blocked
            , teams.away.teamStats.teamSkaterStats.takeaways as away_team_takeaways
            , teams.away.teamStats.teamSkaterStats.giveaways as away_team_giveaways
            , teams.away.teamStats.teamSkaterStats.hits as away_team_hits
        from live_boxscore
    )

select * from final
