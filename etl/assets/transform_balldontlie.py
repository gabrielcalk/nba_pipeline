from pandas import DataFrame, NamedAgg, json_normalize, to_numeric
from datetime import datetime

class TransformBalldontlie:
    def __init__(self, team_data, team_players_data, team_games_data, players_stats_data):
        self.team_data = team_data
        self.team_players_data = team_players_data
        self.team_games_data = team_games_data
        self.players_stats_data = players_stats_data

    def team(self):
        df_team = DataFrame(self.team_data, index=[0])
        df_team.rename(columns={'full_name': 'fullName'}, inplace=True)
        return df_team
    
    def team_players(self):
        df_team = DataFrame(self.team_players_data)
        df_team.rename(columns={'jersey_number': 'jerseyNumber'}, inplace=True)
        df_team['fullName'] = df_team['first_name'] + ' ' + df_team['last_name']
        df_team['teamId'] = df_team['team'].apply(lambda x: x['id'])
        # Handling NaN values and ensuring the 'years_since_draft' is an integer
        df_team['yearsSinceDraft'] = (datetime.now().year - df_team['draft_year']).fillna(-1).astype(int)

        df_final = df_team[['id', 'fullName', 'position', 'height', 'weight', 'jerseyNumber', 'college', 'country', 'yearsSinceDraft', 'teamId']]

        return df_final
    
    def team_games(self, team_id: int):
        df_team = DataFrame(self.team_games_data)
        df_team.rename(columns={'home_team_score': 'homeTeamScore', 'visitor_team_score': 'visitorTeamScore'}, inplace=True)

        df_team['totalPoints'] = df_team['homeTeamScore'] + df_team['visitorTeamScore']

        df_team['opponentTeam'] = df_team.apply(lambda row: row['visitor_team']['full_name'] if row['home_team']['id'] == team_id else row['home_team']['full_name'], axis=1)
        df_team['opponentTeamConference'] = df_team.apply(lambda row: row['visitor_team']['conference'] if row['home_team']['id'] == team_id else row['home_team']['conference'], axis=1)

        df_team['isHomeGame'] = df_team.apply(lambda row: row['home_team']['id'] == team_id, axis=1)
        
        df_team['result'] = df_team.apply(lambda row: ('Win' if (row['isHomeGame'] and row['homeTeamScore'] > row['visitorTeamScore']) or 
                                      (not row['isHomeGame'] and row['visitorTeamScore'] > row['homeTeamScore'])
                                      else 'Loss') if row['status'] == 'Final' else None, axis=1)

        df_team['cumulativeWins'] = (df_team['result'] == 'Win').cumsum()
        df_team['cumulativeLosses'] = (df_team['result'] == 'Loss').cumsum()

        df_final = df_team[['id', 'date', 'season', 'postseason', 'opponentTeam', 'status', 'opponentTeamConference', 'isHomeGame',  'totalPoints', 'homeTeamScore', 'visitorTeamScore', 'result', 'cumulativeWins', 'cumulativeLosses']]
        return df_final
    
    def team_players_performance(self):
        df_team_players_performance = DataFrame(self.players_stats_data)
        df_team_players_performance.rename(columns={
            'min': 'minutesPlayed',
            'fgm': 'fieldGoalsMade',
            'fga': 'fieldGoalsAttempted',
            'fg3m': 'threePointsFieldGoalsMade',
            'fg3a': 'threePointsFieldGoalsAttempted',
            'ftm': 'freeThrowsMade',
            'fta': 'freeThrowsAttempted',
            'oreb': 'offensiveRebounds',
            'dreb': 'defensiveRebounds',
            'reb': 'rebounds',
            'ast': 'assists',
            'stl': 'steals',
            'blk': 'blocks',
            'pf': 'personalFouls',
            'pts': 'points'
        }, inplace=True)

        # Calculate percentages
        df_team_players_performance['fieldGoalPercentage'] = df_team_players_performance.apply(
            lambda row: row['fieldGoalsMade'] / row['fieldGoalsAttempted'] if row['fieldGoalsAttempted'] > 0 else 0,
            axis=1
        )
        df_team_players_performance['threePointsFieldGoalPercentage'] = df_team_players_performance.apply(
            lambda row: row['threePointsFieldGoalsMade'] / row['threePointsFieldGoalsAttempted'] if row['threePointsFieldGoalsAttempted'] > 0 else 0,
            axis=1
        )
        df_team_players_performance['freeThrowsPercentage'] = df_team_players_performance.apply(
            lambda row: row['freeThrowsMade'] / row['freeThrowsAttempted'] if row['freeThrowsAttempted'] > 0 else 0,
            axis=1
        )
        def classify_performance(percentage):
            if percentage < 0.3:
                return 'bad'
            elif 0.3 <= percentage <= 0.5:
                return 'ok'
            else:
                return 'good'

        # Apply performance categorization
        df_team_players_performance['fieldGoalPerformance'] = df_team_players_performance['fieldGoalPercentage'].apply(classify_performance)
        df_team_players_performance['threePointsFieldGoalPerformance'] = df_team_players_performance['threePointsFieldGoalPercentage'].apply(classify_performance)
        df_team_players_performance['freeThrowsPerformance'] = df_team_players_performance['freeThrowsPercentage'].apply(classify_performance)

        # Extract nested data for player, team, and game
        df_team_players_performance['playerId'] = df_team_players_performance['player'].apply(lambda x: x['id'])
        df_team_players_performance['teamId'] = df_team_players_performance['team'].apply(lambda x: x['id'])
        df_team_players_performance['gameId'] = df_team_players_performance['game'].apply(lambda x: x['id'])

        columns_to_keep = ['id', 'gameId', 'playerId', 'teamId', 'minutesPlayed', 'fieldGoalsMade', 
                   'fieldGoalsAttempted', 'fieldGoalPercentage', 'fieldGoalPerformance', 
                   'threePointsFieldGoalsMade', 'threePointsFieldGoalsAttempted', 
                   'threePointsFieldGoalPercentage', 'threePointsFieldGoalPerformance', 
                   'freeThrowsMade', 'freeThrowsAttempted', 'freeThrowsPercentage', 
                   'freeThrowsPerformance', 'offensiveRebounds', 'defensiveRebounds', 
                   'rebounds', 'assists', 'steals', 'blocks', 'personalFouls', 'points']
        
        df_final = df_team_players_performance[columns_to_keep]
        return df_final
    
    def team_players_overall_performance(self):
        df_team_players_performance = json_normalize(self.players_stats_data)
        df_team_players_performance['min'] = to_numeric(df_team_players_performance['min'], errors='coerce')
        
        df_team_players_performance.rename(columns={'player.id': 'playerId'}, inplace=True)

        grouped = df_team_players_performance.groupby('playerId')
        
        performance_stats = grouped.agg(
            totalMinutesPlayed=('min', 'sum'),
            averageMinutesPlayedPerGame=('min', 'mean'),
            totalFieldGoalsAttempted=('fga', 'sum'),
            totalFieldGoalsMade=('fgm', 'sum'),
            totalThreePointsAttempted=('fg3a', 'sum'),
            totalThreePointsMade=('fg3m', 'sum'),
            totalFreeThrowsAttempted=('fta', 'sum'),
            totalFreeThrowsMade=('ftm', 'sum'),
            totalAssists=('ast', 'sum'),
            totalPoints=('pts', 'sum'),
        )

        performance_stats['fieldGoalPercentage'] = performance_stats['totalFieldGoalsMade'] / performance_stats['totalFieldGoalsAttempted']
        performance_stats['threePointsPercentage'] = performance_stats['totalThreePointsMade'] / performance_stats['totalThreePointsAttempted']
        performance_stats['freeThrowsPercentage'] = performance_stats['totalFreeThrowsMade'] / performance_stats['totalFreeThrowsAttempted']

        performance_stats['fieldGoalPercentage'].fillna(0, inplace=True)
        performance_stats['threePointsPercentage'].fillna(0, inplace=True)
        performance_stats['freeThrowsPercentage'].fillna(0, inplace=True)
        
        performance_stats.reset_index(inplace=True)

        performance_stats.rename(columns={'playerId': 'id'}, inplace=True)

        return performance_stats