from jinja2 import Environment
from pandas import DataFrame
from nba_pipeline.etl.connectors.postgresql import PostgreSqlClient

class LoadBalldontlie:
    def __init__(self, tables_template: Environment, postgresql_client: PostgreSqlClient):
        self.tables_template = tables_template
        self.postgresql_client = postgresql_client
       
    def load_team(self, data: DataFrame, table_name: str):
        self.postgresql_client.upsert(data, self.tables_template, table_name)
        
    def load_team_players(self, data: DataFrame, table_name: str):
        self.postgresql_client.upsert(data, self.tables_template, table_name)
        