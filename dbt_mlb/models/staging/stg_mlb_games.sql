with source as (
    select 
        raw_json,
        load_timestamp,
        filename
    from {{ source('raw', 'mlb_games') }}
),

extracted as (
    SELECT
        f.value:game_id::VARCHAR       AS game_id,
        f.value:search_start_date::VARCHAR AS search_start_date,
        f.value:search_end_date::VARCHAR   AS search_end_date,
        f.value:chosen_team_name::VARCHAR  AS chosen_team_name,
        f.value:home_team::VARCHAR         AS home_team,
        f.value:away_team::VARCHAR         AS away_team,
        f.value:home_score::INTEGER        AS home_score,
        f.value:away_score::INTEGER        AS away_score,
        f.value:game_time::VARCHAR         AS game_time_raw,
        ABS(f.value:home_score::INTEGER - f.value:away_score::INTEGER) AS score_diff,
        load_timestamp,
        filename
    FROM source
    , LATERAL FLATTEN(input => raw_json) AS f
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
    score_diff,
    load_timestamp,
    filename
from extracted