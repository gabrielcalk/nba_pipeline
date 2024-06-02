from jinja2 import Environment
from pandas import DataFrame
from nba_pipeline.etl.connectors.postgresql import PostgreSqlClient

class LoadBalldontlie:
    def __init__(self, tables_template: Environment, team_name: str, season: int, postgresql_client: PostgreSqlClient):
        self.tables_template = tables_template
        self.postgresql_client = postgresql_client
        self.team_name = team_name
        self.season = season
       
    def load_team(self, data: DataFrame, file_name: str):
        table_name = f"{self.team_name}_{file_name}"
        self.postgresql_client.upsert(data, self.tables_template, table_name, file_name)
        
    def load_team_players(self, data: DataFrame, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.postgresql_client.upsert(data, self.tables_template, table_name, file_name)
        
    def load_team_games(self, data: DataFrame, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.postgresql_client.upsert(data, self.tables_template, table_name, file_name)
    
    def load_players_performance(self, data: DataFrame, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.postgresql_client.upsert(data, self.tables_template, table_name, file_name)
    
    def load_players_overall_performance(self, data: DataFrame, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.postgresql_client.upsert(data, self.tables_template, table_name, file_name)
        