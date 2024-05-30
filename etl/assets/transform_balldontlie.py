from pandas import DataFrame
from datetime import datetime

class TransformBalldontlie:
    def __init__(self, team_data, team_players_data, team_games_data, players_stats_data):
        self.team_data = team_data
        self.team_players_data = team_players_data
        self.team_games = team_games_data
        self.players_stats = players_stats_data

    def team(self):
        df_team = DataFrame(self.team_data, index=[0])
        df_team.rename(columns={'full_name': 'fullName'}, inplace=True)
        return df_team
    
    def team_employees(self):
        df_team = DataFrame(self.team_players_data)
        df_team.rename(columns={'jersey_number': 'jerseyNumber'}, inplace=True)
        df_team['fullName'] = df_team['first_name'] + ' ' + df_team['last_name']
        df_team['teamId'] = df_team['team'].apply(lambda x: x['id'])
        # Handling NaN values and ensuring the 'years_since_draft' is an integer
        df_team['yearsSinceDraft'] = (datetime.now().year - df_team['draft_year']).fillna(-1).astype(int)
        df_team['isCurrentPlayer'] = df_team['position'].apply(lambda x: x.strip() != '')

        df_final = df_team[['id', 'fullName', 'position', 'height', 'weight', 'jerseyNumber', 'college', 'country', 'yearsSinceDraft', 'teamId', 'isCurrentPlayer']]

        return df_final