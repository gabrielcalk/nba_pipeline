from dotenv import load_dotenv
from pathlib import Path
import yaml
import os
from nba_pipeline.etl.assets.extract_balldontlie import ExtractBalldontlie
# from etl.assets.transform_balldontlie import TransformBalldontlie
# from etl.assets.load_balldontlie import LoadBalldontlie
# from etl.connectors.postgresql import PostgreSqlClient

def get_config():
    # Get the path of the current script and replace the extension
    yaml_file_path = __file__.replace(".py", ".yaml")
    # Check if the YAML file exists
    if Path(yaml_file_path).exists():
        # Open and read the YAML file
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            return pipeline_config
    else:
        # Raise an exception if the YAML file is not found
        raise Exception(f"Missing {yaml_file_path} file")

if __name__ == "__main__":
    load_dotenv()
    
    # Load the configuration
    config = get_config()
    # Retrieve config
    team_id = config.get("team_id")
    start_date = config.get("start_date")
    end_date = config.get("end_date")
    api_url = config.get("api_url")

    if not team_id:
        raise ValueError("Invalid or missing 'team_id' in configuration.")
    
    if not start_date:
        raise ValueError("Invalid or missing 'start_date' in configuration.")
    
    if not end_date:
        raise ValueError("Invalid or missing 'end_date' in configuration.")
    
    BALL_DONT_LIE_API_KEY =  os.environ.get("BALL_DONT_LIE_API_KEY")

    try:
        # Extracting
        extractor = ExtractBalldontlie(BALL_DONT_LIE_API_KEY, api_url, team_id, start_date, end_date)
        team = extractor.extract_team()
        team_players = extractor.extract_players()
        team_games = extractor.extract_games()
        games_ids = [game.get("id") for game in team_games]
        players_stats = extractor.extract_players_stats(games_ids)
        print(players_stats)

        # Transforming
        # transformer = TransformBalldontlie(df_team_players, df_team_games, df_players_stats)
        # df_players_performance = transformer.players_performance()
        # df_team_performance = transformer.team_performance()

        # # Loading
        # SERVER_NAME = os.environ.get("DB_SERVER_NAME")
        # DATABASE_NAME = os.environ.get("DB_NAME")
        # DB_USERNAME = os.environ.get("DB_USERNAME")
        # DB_PASSWORD = os.environ.get("DB_PASSWORD")
        # PORT = os.environ.get("DB_PORT")

        # postgresql_client = PostgreSqlClient(
        #     server_name=SERVER_NAME,
        #     database_name=DATABASE_NAME,
        #     username=DB_USERNAME,
        #     password=DB_PASSWORD,
        #     port=PORT,
        # )

        # team_name = df_team.get("name")
        # season = start_date.split("-")[0]
        
        # team_table = postgresql_client.create_table(df_team, f"{team_name}_team_{season}")
        # team_players_table = postgresql_client.create_table(df_team_players, f"{team_name}_players_{season}")
        # team_games_table = postgresql_client.create_table(df_team_games, f"{team_name}_games_{season}")
        # team_stats_table = postgresql_client.create_table(df_players_stats, f"{team_name}_players_stats_{season}")
        # team_performance_table = postgresql_client.create_table(df_team_performance, f"{team_name}_team_performance_{season}")
        # players_performance_table = postgresql_client.create_table(df_players_performance, f"{team_name}_players_performance_{season}")

        # loader = LoadBalldontlie()
        # loader.load(df_team, team_table)
        # loader.load(df_team_players, team_players_table)
        # loader.load(df_team_games, team_games_table)
        # loader.load(df_players_stats, team_stats_table)
        # loader.load(df_team_performance, team_performance_table)
        # loader.load(df_players_performance, players_performance_table)

    except BaseException as e:
        print(f"Pipeline run failed. See detailed logs: {e}")
        # pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        # metadata_logger.log(
        #     status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        # )  # log
