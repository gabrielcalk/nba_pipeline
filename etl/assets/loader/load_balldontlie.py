from jinja2 import Environment
from pandas import DataFrame
from etl.connectors.postgresql import PostgreSqlClient
from logging import Logger

class LoadBalldontlie:
    def __init__(
        self, 
        logger: Logger, 
        tables_template: Environment, 
        team_name: str, 
        season: int, 
        sql_client: PostgreSqlClient,
        df_team: DataFrame,
        df_team_players: DataFrame,
        df_team_games: DataFrame,
        df_players_performance: DataFrame,
        df_players_overall_performance: DataFrame,
        chunk_size: int = 500,
    ):
        self.tables_template = tables_template
        self.sql_client = sql_client
        self.logger = logger
        self.team_name = team_name
        self.season = season
        self.df_team = df_team
        self.df_team_players = df_team_players
        self.df_team_games = df_team_games
        self.df_players_performance = df_players_performance
        self.df_players_overall_performance = df_players_overall_performance
        self.chunk_size = chunk_size
        
    def load(self, mode: str):
        if(mode == "full"):
            self.sql_client.drop_tables()
        self.load_team("team")
        self.load_team_players("players")
        self.load_team_games("games")
        self.load_players_performance("players_performance")
        self.load_players_overall_performance("players_overall_performance")
  
    def load_team(self, file_name: str):
        table_name = f"{self.team_name}_{file_name}"
        self.logger.info(f"Loaded team data. Size: {len(self.df_team)}. Table: {table_name}")
        self.sql_client.upsert(self.df_team, self.tables_template, table_name, file_name, self.chunk_size)
        
    def load_team_players(self, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.logger.info(f"Loaded team players data. Size: {len(self.df_team_players)}. Table: {table_name}")
        self.sql_client.upsert(self.df_team_players, self.tables_template, table_name, file_name, self.chunk_size)
        
    def load_team_games(self, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.logger.info(f"Loaded team games data. Size: {len(self.df_team_games)}. Table: {table_name}")
        self.sql_client.upsert(self.df_team_games, self.tables_template, table_name, file_name, self.chunk_size)
    
    def load_players_performance(self, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.logger.info(f"Loaded players performance data. Size: {len(self.df_players_performance)}. Table: {table_name}")
        self.sql_client.upsert(self.df_players_performance, self.tables_template, table_name, file_name, self.chunk_size)
    
    def load_players_overall_performance(self, file_name: str):
        table_name = f"{self.team_name}_{self.season}_{file_name}"
        self.logger.info(f"Loaded players overall performance data. Size: {len(self.df_players_overall_performance)}. Table: {table_name}")
        self.sql_client.upsert(self.df_players_overall_performance, self.tables_template, table_name, file_name, self.chunk_size)
    
    