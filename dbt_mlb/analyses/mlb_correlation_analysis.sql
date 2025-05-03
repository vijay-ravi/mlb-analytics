-- Snowflake has built-in correlation function
with correlation_data as (
    select
        search_start_date,
        search_end_date,
        chosen_team_name,
        CORR(game_time_in_minutes, score_diff) as time_differential_correlation
    from {{ ref('stg_mlb_game_times') }}
    group by
        search_start_date,
        search_end_date,
        chosen_team_name
)

select * from correlation_data