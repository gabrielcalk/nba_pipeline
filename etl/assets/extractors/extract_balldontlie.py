import requests
from logging import Logger
from etl.connectors.postgresql import PostgreSqlClient

class ExtractBalldontlie:
    def __init__(self, sql_client: PostgreSqlClient, logger: Logger, api_key: str, api_url: str, team_id: int, season: str, mode: str):
        self.api_key = api_key
        self.team_id = team_id
        self.season = season
        self.base_url = api_url
        self.logger = logger
        self.sql_client = sql_client
        self.mode = mode
        
    def extract(self):
        team = self.extract_team()
        self.team_name = team.get('name')
        self.logger.info(f"Extracted team data: {self.team_name}")
        
        team_players = self.extract_players()
        self.logger.info(f"Extracted players data on season {self.season}. Size: {len(team_players)}")
        
        players_ids = [player.get("id") for player in team_players]
        self.logger.info(f"Extracted players ids. players_ids: {players_ids}")
        
        team_games = self.extract_games()
        self.logger.info(f"Extracted games data on season {self.season}. Size: {len(team_games)}")
        
        players_stats = self.extract_players_stats(players_ids)
        self.logger.info(f"Extracted players stats data on season {self.season}. Size: {len(players_stats)}")
        
        return team, team_players, team_games, players_stats

    def extract_players(self):
        url = f"{self.base_url}/players"
        params = {"team_ids[]": self.team_id}
        data = self._fetch_data(url, params)
        players_data = data.get("data")
        players_with_position = [player for player in players_data if player['position'] != '']
        return players_with_position

    def extract_team(self):
        url = f"{self.base_url}/teams/{self.team_id}"
        data = self._fetch_data(url)
        return data.get("data")

    def extract_games(self):
        cursor = 0
        if self.mode == "increment":
            cursor = self.sql_client.select_max_id(f"{self.team_name}_{self.season}_games")
            
        url = f"{self.base_url}/games"
        params = {
            "team_ids[]": self.team_id,
            "seasons[]=2023": self.season,
            "per_page": 100
        }
        collected_data = []
        self._fetch_pagination_data(url=url, collected_data=collected_data, params=params, next_cursor=cursor)
            
        return collected_data

    def extract_players_stats(self, players_ids: list[int]):
        cursor = 0
        if self.mode == "increment":
            cursor = self.sql_client.select_max_id(f"{self.team_name}_{self.season}_players_performance")
            
        url = f"{self.base_url}/stats"
        params = {
            "seasons[]": self.season,
            "player_ids[]": players_ids,
            "per_page": 100 
        }
        
        collected_data = []
        self._fetch_pagination_data(url=url, collected_data=collected_data, params=params, next_cursor=cursor)
       
        return collected_data


    def _fetch_data(self, url: str, params=None):
        headers = {
            "Authorization": f"{self.api_key}"
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to fetch data. Status Code: {response.status_code}. Response: {response.text}"
            )
            
    def _fetch_pagination_data(self, url: str, collected_data, params=None, next_cursor=None):
        if(next_cursor):
            params['cursor'] = next_cursor
            
        while True:
            response = self._fetch_data(url, params)
            collected_data.extend(response.get('data', []))
            next_cursor = response.get('meta', {}).get('next_cursor')
            if not next_cursor:  
                break
            params['cursor'] = next_cursor
            
    
