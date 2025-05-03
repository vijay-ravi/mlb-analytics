from prefect import flow

SOURCE_REPO="https://github.com/vijay-ravi/mlb-analytics.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="prefect_mlb/flows/mlb_flow.py:mlb_flow",
    ).deploy(
        name="MLB Analytics Deployment",
        version="1.0",
        parameters={
            "team_name": "marlins",
            "start_date": "06/01/2024",
            "end_date": "06/30/2024"
        },
        work_pool_name="dev-pool",
        cron="0 * * * *",  # Run every hour
        tags=["mlb", "production"]
    )
