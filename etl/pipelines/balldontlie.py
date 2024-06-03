from dotenv import load_dotenv
from pathlib import Path
import yaml
import os
from etl.assets.extractors.extract_balldontlie import ExtractBalldontlie
from etl.assets.tranformers.transform_balldontlie import TransformBalldontlie
from etl.assets.loader.load_balldontlie import LoadBalldontlie
from etl.connectors.postgresql import PostgreSqlClient
from etl.connectors.logger import get_logger
from jinja2 import Environment, FileSystemLoader
from etl.connectors.config_manager import get_parameter

def get_config():
    isDevelopment = os.environ.get("ENV") == "dev"
    if(isDevelopment):
        yaml_file_path = __file__.replace(".py", ".yaml")
       
        if Path(yaml_file_path).exists():
            with open(yaml_file_path) as yaml_file:
                pipeline_config = yaml.safe_load(yaml_file)
                return pipeline_config
        else:
            raise Exception(f"Missing {yaml_file_path} file")
        
    print(os.environ.get("AWS_ACCESS_KEY_ID"))
    print(os.environ.get("AWS_SECRET_ACCESS_KEY"))
        
    config = {
        "team_id": get_parameter('team_id'),
        "season": get_parameter('season'),
        "api_url": get_parameter('api_url')
    }
    
    return config

if __name__ == "__main__":
    load_dotenv()
    
    config = get_config()
    team_id = config.get("team_id")
    season = config.get("season")
    api_url = config.get("api_url")
    
    SERVER_NAME = os.environ.get("DB_SERVER_NAME")
    DATABASE_NAME = os.environ.get("DB_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("DB_PORT")
    MODE = os.environ.get("MODE", "increment")
    
    BALL_DONT_LIE_API_KEY = os.environ.get("BALL_DONT_LIE_API_KEY")
    
    tables_template = Environment(
        loader=FileSystemLoader("etl/assets/sql")
    )

    if not team_id:
        raise ValueError("Invalid or missing 'team_id' in configuration.")
    
    if not season:
        raise ValueError("Invalid or missing 'season' in configuration.")
    
    if not api_url:
        raise ValueError("Invalid or missing 'api_url' in configuration.")
    
    if not BALL_DONT_LIE_API_KEY:
        raise ValueError("Invalid or missing 'BALL_DONT_LIE_API_KEY' in environment variables.")
    
    if not SERVER_NAME or not DATABASE_NAME or not DB_USERNAME or not DB_PASSWORD:
        raise ValueError("Invalid or missing database configuration in environment variables.")

    if not tables_template:
        raise ValueError("Invalid or missing 'tables_template' in configuration.")
    
    logger = get_logger(
        name='nba_pipeline_log', log_group='nba_pipeline_log_group', stream_name='nba_pipeline_log_stream'
    )
    
    sql_client = PostgreSqlClient(
        logger=logger,
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    
    logger.info(f"Starting pipeline run. Team ID: {team_id}, Season: {season}, API URL: {api_url}, Mode: {MODE}") 

    try:
        # Extracting
        extractor = ExtractBalldontlie(
            sql_client = sql_client,
            mode = MODE,
            api_key=BALL_DONT_LIE_API_KEY, 
            api_url=api_url, 
            team_id=team_id, 
            season=season,
            logger=logger
        )
        
        team, team_players, team_games, players_stats = extractor.extract()
        team_name = team.get("name").lower()

        # Transforming
        transformer = TransformBalldontlie(
            team_data=team, 
            team_players_data=team_players, 
            team_games_data=team_games, 
            players_stats_data=players_stats,
            logger=logger
        )
       
        df_team, df_team_players, df_team_games, df_players_performance, df_players_overall_performance = transformer.transform()

        # Loading        
        loader = LoadBalldontlie(
            tables_template=tables_template, 
            team_name=team_name, 
            season=season, 
            sql_client=sql_client,
            logger=logger,
            df_team=df_team,
            df_team_players=df_team_players,
            df_team_games=df_team_games,
            df_players_performance=df_players_performance,
            df_players_overall_performance=df_players_overall_performance,
        )
        
        loader.load()
        
        logger.info("Pipeline run successfully.")
    except Exception as e:
        logger.error(f"Pipeline run failed. See detailed logs: {e}")
       

