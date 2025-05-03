from prefect import task
import statsapi
import json
import boto3
from datetime import datetime
import logging

from prefect.blocks.aws import AwsCredentials, S3Bucket

# Load AWS credentials and S3 bucket from Prefect blocks
aws_creds = AwsCredentials.load("prefect-aws-credentials")
s3_bucket = S3Bucket.load("prefect-s3-bucket")

AWS_ACCESS_KEY_ID = aws_creds.access_key_id
AWS_SECRET_ACCESS_KEY = aws_creds.secret_access_key
S3_BUCKET_NAME = s3_bucket.bucket
S3_PREFIX = "mlb_data"

@task(name="Get Recent MLB Games", retries=3, retry_delay_seconds=30)
def get_recent_games(team_name, start_date, end_date):
    '''This task will fetch the schedule for the provided team and date range and return the game ids.'''
    logging.info(f"Fetching games for {team_name} from {start_date} to {end_date}")
    team = statsapi.lookup_team(team_name)
    schedule = statsapi.schedule(team=team[0]["id"], start_date=start_date, end_date=end_date)
    return [game['game_id'] for game in schedule]

@task(name="Fetch Game Boxscore", retries=2, retry_delay_seconds=60)
def fetch_single_game_boxscore(game_id, start_date, end_date, team_name):
    '''This task will fetch the boxscore for a single game and return the game data.'''
    logging.info(f"Fetching boxscore for game {game_id}")
    boxscore = statsapi.boxscore_data(game_id)
    
    # Extract relevant data
    home_score = boxscore['home']['teamStats']['batting']['runs']
    away_score = boxscore['away']['teamStats']['batting']['runs']
    home_team = boxscore['teamInfo']['home']['teamName']
    away_team = boxscore['teamInfo']['away']['teamName']
    time_value = next(item['value'] for item in boxscore['gameBoxInfo'] if item['label'] == 'T')
    
    # Create a dictionary with the game data
    game_data = {
        'search_start_date': start_date,
        'search_end_date': end_date,
        'chosen_team_name': team_name,
        'game_id': game_id,
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score,
        'score_differential': abs(home_score - away_score),
        'game_time': time_value,
    }
    
    return game_data

@task(name="Upload Data to S3", retries=3, retry_delay_seconds=30)
def upload_data_to_s3(game_data, team_name):
    '''This task uploads the data to S3 instead of saving locally.'''
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # Create a filename for S3
    today = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"{S3_PREFIX}/{today}/{team_name}-boxscore-{datetime.now().strftime('%H%M%S')}.json"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(game_data),
        ContentType='application/json'
    )
    
    logging.info(f"Uploaded data to s3://{S3_BUCKET_NAME}/{s3_key}")
    return f"s3://{S3_BUCKET_NAME}/{s3_key}", today