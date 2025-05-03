from prefect import task
from prefect.artifacts import create_markdown_artifact
import pandas as pd
import snowflake.connector
import logging
from sqlalchemy import create_engine
from prefect_mlb.utils.config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA_ANALYTICS, SNOWFLAKE_SCHEMA_STAGING, SNOWFLAKE_SCHEMA_RAW,
    get_snowflake_connection_string
)

@task(name="Create Analysis Artifact")
def create_game_analysis_artifact(team_name, start_date, end_date):
    '''This task will query Snowflake and create a Prefect artifact with the game analysis.'''
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_ANALYTICS,
        role=SNOWFLAKE_ROLE
    )
    
    try:
        cursor = conn.cursor()
        
        # Query for analysis results
        analytics_query = f"""
        SELECT * 
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_ANALYTICS}.MLB_GAME_ANALYSIS
        WHERE chosen_team_name = '{team_name}'
        AND search_start_date = '{start_date}'
        AND search_end_date = '{end_date}'
        ORDER BY analysis_timestamp DESC 
        LIMIT 1
        """
        
        cursor.execute(analytics_query)
        analysis_result = cursor.fetchone()
        column_names = [desc[0] for desc in cursor.description]
        analysis_data = dict(zip(column_names, analysis_result))
        
        # Query for correlation from the analysis
        correlation_query = f"""
        SELECT time_differential_correlation 
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_RAW}.MLB_CORRELATION_ANALYSIS
        WHERE chosen_team_name = '{team_name}'
        AND search_start_date = '{start_date}'
        AND search_end_date = '{end_date}'
        LIMIT 1
        """
        
        cursor.execute(correlation_query)
        correlation_result = cursor.fetchone()
        correlation = correlation_result[0] if correlation_result else None
        
        # Query for raw game data
        games_query = f"""
        SELECT 
            game_id, home_team, away_team, home_score, away_score, 
            score_differential, game_time_raw, game_time_in_minutes 
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_STAGING}.STG_MLB_GAME_TIMES
        WHERE chosen_team_name = '{team_name}'
        AND search_start_date = '{start_date}'
        AND search_end_date = '{end_date}'
        """
        
        cursor.execute(games_query)
        games_data = cursor.fetchall()
        games_columns = [desc[0] for desc in cursor.description]
        games_df = pd.DataFrame(games_data, columns=games_columns)
        
        # Create markdown report
        markdown_report = f"""# MLB Game Analysis Report
## Search Parameters
Search Start Date: {analysis_data['SEARCH_START_DATE']}
Search End Date: {analysis_data['SEARCH_END_DATE']}
Chosen Team Name: {analysis_data['CHOSEN_TEAM_NAME']}

## Summary Statistics
Max game time: {analysis_data['MAX_GAME_TIME']:.2f} minutes
Min game time: {analysis_data['MIN_GAME_TIME']:.2f} minutes
Median game time: {analysis_data['MEDIAN_GAME_TIME']:.2f} minutes
Average game time: {analysis_data['AVERAGE_GAME_TIME']:.2f} minutes
Max differential: {analysis_data['MAX_DIFFERENTIAL']:.2f}
Min differential: {analysis_data['MIN_DIFFERENTIAL']:.2f}
Median differential: {analysis_data['MEDIAN_DIFFERENTIAL']:.2f}
Average differential: {analysis_data['AVERAGE_DIFFERENTIAL']:.2f}
Correlation between game time and score differential: {correlation:.2f}

## Raw Data
{games_df.to_markdown(index=False)}
"""
        
        create_markdown_artifact(
            key="game-analysis",
            markdown=markdown_report,
            description="MLB Game Analysis Report"
        )
        
        return analysis_data
        
    finally:
        conn.close()