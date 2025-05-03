with game_stats as (
    select
        search_start_date,
        search_end_date,
        chosen_team_name,
        
        -- Calculate basic statistics on game time
        MAX(game_time_in_minutes) as max_game_time,
        MIN(game_time_in_minutes) as min_game_time,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY game_time_in_minutes) as median_game_time,
        AVG(game_time_in_minutes) as average_game_time,
        
        -- Calculate basic statistics on score differential
        MAX(score_diff) as max_differential,
        MIN(score_diff) as min_differential,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score_diff) as median_differential,
        AVG(score_diff) as average_differential
    from {{ ref('stg_mlb_game_times') }}
    group by 
        search_start_date,
        search_end_date,
        chosen_team_name
)

select
    search_start_date,
    search_end_date,
    chosen_team_name,
    max_game_time,
    min_game_time,
    median_game_time,
    average_game_time,
    max_differential,
    min_differential,
    median_differential,
    average_differential,
    -- We'll calculate correlation in the next model or in Python
    NULL as time_differential_correlation,
    CURRENT_TIMESTAMP() as analysis_timestamp
from game_stats