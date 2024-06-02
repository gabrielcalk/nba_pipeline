import requests

class ExtractBalldontlie:
    def __init__(self, api_key, api_url, team_id, season):
        self.api_key = api_key
        self.team_id = team_id
        self.season = season
        self.base_url = api_url

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
        url = f"{self.base_url}/games"
        params = {
            "team_ids[]": self.team_id,
            "seasons[]=2023": self.season,
            "per_page": 100
        }
        data = self._fetch_data(url, params)
        return data.get("data")

    def extract_players_stats(self, players_ids):
        """
        Extracts player statistics for a list of player IDs for the current season.
        
        Args:
            players_ids (list): A list of player IDs to fetch stats for.
        
        Returns:
            list: A list of dictionaries containing player statistics.
        """
        url = f"{self.base_url}/stats"
        params = {
            "seasons[]": self.season,
            "player_ids[]": players_ids,
            "per_page": 100 
        }
        
        collected_data = []
        while True:
            response = self._fetch_data(url, params)
            collected_data.extend(response.get('data', []))
            next_cursor = response.get('meta', {}).get('next_cursor')
            if not next_cursor:  
                break
            params['cursor'] = next_cursor
        
        return collected_data


    def _fetch_data(self, url, params=None):
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
