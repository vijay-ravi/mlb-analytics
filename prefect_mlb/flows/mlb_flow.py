from prefect import flow
from datetime import datetime
import os
import logging

from prefect_mlb.tasks.extract_tasks import get_recent_games, fetch_single_game_boxscore, upload_data_to_s3
from prefect_mlb.tasks.load_tasks import load_data_to_snowflake
from prefect_mlb.tasks.artifact_tasks import create_game_analysis_artifact

@flow(name="MLB Analytics Flow")
def mlb_flow(team_name: str, start_date: str, end_date: str):
    """
    Main flow that orchestrates the MLB data pipeline with Snowflake and dbt.
    
    Args:
        team_name: The MLB team name (e.g., "marlins")
        start_date: Start date for game data (MM/DD/YYYY)
        end_date: End date for game data (MM/DD/YYYY)
    """
    logging.info(f"Starting MLB flow for {team_name} from {start_date} to {end_date}")
    
    # Step 1: Extract - Get game IDs and boxscores
    game_ids = get_recent_games(team_name, start_date, end_date)
    
    if not game_ids:
        logging.warning(f"No games found for {team_name} from {start_date} to {end_date}")
        return
    
    # Step 2: Extract - Fetch boxscore for each game
    game_data = []
    for game_id in game_ids:
        boxscore = fetch_single_game_boxscore(game_id, start_date, end_date, team_name)
        game_data.append(boxscore)
    
    # Step 3: Load - Upload data to S3 (Bronze layer)
    s3_path, today = upload_data_to_s3(game_data, team_name)
    
    # Step 4: Load - Copy data from S3 to Snowflake Raw (Bronze layer)
    rows_loaded = load_data_to_snowflake(s3_path, today)
    logging.info(f"Loaded {rows_loaded} rows to Snowflake Raw layer")

    return s3_path
    
    # # Step 5: Transform - Run dbt models (Silver and Gold layers)
    # dbt_success = run_dbt_models()
    
    # if dbt_success:
    #     # Step 6: Create analysis artifact with results from Snowflake
    #     analysis = create_game_analysis_artifact(team_name, start_date, end_date)
    #     logging.info(f"Analysis complete for {team_name}")
    #     return analysis
    # else:
    #     logging.error("dbt transformations failed")
    #     return None

if __name__ == "__main__":
    mlb_flow("marlins", "06/01/2024", "06/30/2024")
    
    # For scheduled deployment:
    # mlb_flow.serve(
    #     parameters={
    #         "team_name": "marlins",
    #         "start_date": "06/01/2024",
    #         "end_date": "06/30/2024"
    #     },
    #     cron="30 * * * *"
    # )