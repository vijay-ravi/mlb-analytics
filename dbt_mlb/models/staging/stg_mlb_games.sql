with source as (
    select 
        raw_json,
        load_timestamp,
        filename
    from {{ source('raw', 'mlb_games') }}
),

extracted as (
    select
        raw_json:game_id::VARCHAR as game_id,
        raw_json:search_start_date::VARCHAR as search_start_date,
        raw_json:search_end_date::VARCHAR as search_end_date,
        raw_json:chosen_team_name::VARCHAR as chosen_team_name,
        raw_json:home_team::VARCHAR as home_team,
        raw_json:away_team::VARCHAR as away_team,
        raw_json:home_score::INTEGER as home_score,
        raw_json:away_score::INTEGER as away_score,
        raw_json:game_time::VARCHAR as game_time_raw,
        ABS(raw_json:home_score::INTEGER - raw_json:away_score::INTEGER) as score_differential,
        load_timestamp,
        filename
    from source
)

select
    game_id,
    search_start_date,
    search_end_date,
    chosen_team_name,
    home_team,
    away_team,
    home_score,
    away_score,
    game_time_raw,
    score_differential,
    load_timestamp,
    filename
from extracted