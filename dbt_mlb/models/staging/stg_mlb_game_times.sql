with game_base as (
    select 
        *,
        -- First remove any text in parentheses (like delay notes)
        REGEXP_REPLACE(game_time_raw, '\\(.*\\)', '') as game_time_no_parentheses
    from {{ ref('stg_mlb_games') }}
),

cleaned as (
    select
        *,
        -- Extract only digits and colon
        REGEXP_REPLACE(game_time_no_parentheses, '[^0-9:]', '') as game_time_clean,
        -- Split hours and minutes
        SPLIT_PART(REGEXP_REPLACE(game_time_no_parentheses, '[^0-9:]', ''), ':', 1)::INTEGER as hours,
        SPLIT_PART(REGEXP_REPLACE(game_time_no_parentheses, '[^0-9:]', ''), ':', 2)::INTEGER as minutes
    from game_base
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
    score_diff,
    game_time_raw,
    game_time_clean,
    -- Calculate game time in minutes
    (hours * 60 + minutes) as game_time_in_minutes,
    load_timestamp,
    filename
from cleaned