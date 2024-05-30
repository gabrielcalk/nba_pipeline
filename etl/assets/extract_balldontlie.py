import requests

class ExtractBalldontlie:
    def __init__(self, api_key, api_url, team_id, start_date, end_date):
        self.api_key = api_key
        self.team_id = team_id
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = api_url

    def extract_employees(self):
        url = f"{self.base_url}/players"
        params = {"team_ids[]": self.team_id}
        data = self._fetch_data(url, params)
        return data

    def extract_team(self):
        url = f"{self.base_url}/teams/{self.team_id}"
        data = self._fetch_data(url)
        return data

    def extract_games(self):
        url = f"{self.base_url}/games"
        params = {
            "team_ids[]": self.team_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "per_page": 100
        }
        data = self._fetch_data(url, params)
        return data

    def extract_players_stats(self, game_ids):
        url = f"{self.base_url}/stats"
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "per_page": 100,
            "game_ids[]": game_ids
        }
        data = self._fetch_data(url, params)
        return data

    def transform_data(self):
        print("Data transformed")

    def _fetch_data(self, url, params=None):
        headers = {
            "Authorization": f"{self.api_key}"
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200:
            return response.json().get("data")
        else:
            raise Exception(
                f"Failed to fetch data. Status Code: {response.status_code}. Response: {response.text}"
            )
