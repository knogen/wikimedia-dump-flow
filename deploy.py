from flow import wikimedia_dumper
from pathlib import Path

from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig, 
    ConcurrencyLimitStrategy
)

if __name__ == "__main__":
    wikimedia_dumper.from_source(
        source=str(Path(__file__).parent),
        entrypoint="flow.py:wikimedia_dumper",
    ).deploy(
        name="wikimedia-downloader",
        work_pool_name="process-pool",

        concurrency_limit=ConcurrencyLimitConfig(
            limit=3, collision_strategy=ConcurrencyLimitStrategy.ENQUEUE
        ),
        parameters={"output_folder": "/mnt/st01/wikipedia_download", "proxy": "http://192.168.1.230:10808"},
        # set env
        # job_variables={"env": {"NAME": "Marvin"}},
        tags=["wikimedia", "crawler"],
        cron="39 17 * * *",
    )
    