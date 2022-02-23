SELECT 
    /* Primary Key */
    b.game_id as id
    
    /* Foreign Keys */
    ,b.teams.away.team.id as away_team_id
    ,b.teams.home.team.id as home_team_id

    /* Properties */
    ,b.teams.away.teamStats.teamSkaterStats.goals as away_team_goals
    ,b.teams.away.teamStats.teamSkaterStats.pim as away_team_pim
    ,b.teams.away.teamStats.teamSkaterStats.shots as away_team_shots
    ,b.teams.away.teamStats.teamSkaterStats.powerPlayGoals as away_team_powerplay_goals
    ,b.teams.away.teamStats.teamSkaterStats.powerPlayOpportunities as away_team_powerplay_opportunities
    ,b.teams.away.teamStats.teamSkaterStats.faceOffWinPercentage as away_team_faceoff_percentage
    ,b.teams.away.teamStats.teamSkaterStats.blocked as away_team_blocked
    ,b.teams.away.teamStats.teamSkaterStats.takeaways as away_team_takeaways
    ,b.teams.away.teamStats.teamSkaterStats.giveaways as away_team_giveaways
    ,b.teams.away.teamStats.teamSkaterStats.hits as away_team_hits
    ,b.teams.home.teamStats.teamSkaterStats.goals as home_team_goals
    ,b.teams.home.teamStats.teamSkaterStats.pim as home_team_pim
    ,b.teams.home.teamStats.teamSkaterStats.shots as home_team_shots
    ,b.teams.home.teamStats.teamSkaterStats.powerPlayGoals as home_team_powerplay_goals
    ,b.teams.home.teamStats.teamSkaterStats.powerPlayOpportunities as home_team_powerplay_opportunities
    ,b.teams.home.teamStats.teamSkaterStats.faceOffWinPercentage as home_team_faceoff_percentage
    ,b.teams.home.teamStats.teamSkaterStats.blocked as home_team_blocked
    ,b.teams.home.teamStats.teamSkaterStats.takeaways as home_team_takeaways
    ,b.teams.home.teamStats.teamSkaterStats.giveaways as home_team_giveaways
    ,b.teams.home.teamStats.teamSkaterStats.hits as home_team_hits

FROM 
    {{ ref('live_boxscore') }} as b

WHERE 1 = 1

ORDER BY 
    b.game_id desc