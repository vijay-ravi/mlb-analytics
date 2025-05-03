from prefect import task
import snowflake.connector
import logging
from sqlalchemy import create_engine
import os
from ..utils.config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA_RAW, get_snowflake_connection_string
)

@task(name="Load Data to Snowflake", retries=3, retry_delay_seconds=60)
def load_data_to_snowflake(s3_path):
    '''This task will load data from S3 to Snowflake using COPY command.'''
    # Extract filename from S3 path
    filename = s3_path.split('/')[-1]
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_RAW,
        role=SNOWFLAKE_ROLE
    )
    
    try:
        cursor = conn.cursor()
        
        # Execute COPY command to load data from S3 stage to Snowflake
        copy_command = f"""
        COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_RAW}.MLB_GAMES (RAW_JSON, FILENAME)
        FROM (
            SELECT 
                $1, 
                '{filename}'
            FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_RAW}.MLB_S3_STAGE/{filename}
        )
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'CONTINUE';
        """
        
        cursor.execute(copy_command)
        logging.info(f"Loaded data from {s3_path} to Snowflake")
        
        # Return number of rows loaded
        result = cursor.fetchone()
        return result[0] if result else 0
        
    finally:
        cursor.close()
        conn.close()

# @task(name="Run dbt Models", retries=2, retry_delay_seconds=30)
# def run_dbt_models():
#     '''This task runs dbt models to transform the data.'''
#     dbt_project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../dbt"))
    
#     # Change to the dbt project directory
#     os.chdir(dbt_project_dir)
    
#     # Run dbt
#     result = os.system("dbt run --profiles-dir .")
    
#     if result != 0:
#         raise Exception("dbt run failed")
    
#     logging.info("dbt models executed successfully")
#     return True