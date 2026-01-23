from datetime import datetime

from prefect import flow, task, get_run_logger
import random
import time
from typing import List

from prefect.schedules import Cron


date = datetime.now().date()

@task(retries=3, retry_delay_seconds=5)
def get_customer_ids() -> List[str]:
    """Fetch with retry logic"""
    logger = get_run_logger()
    logger.info("Fetching customer IDs...")
    if random.random() < 0.1:  # 10% fetch failure
        raise ValueError("API down")
    ids = [f"customer{random.randint(1, 100)}" for _ in range(10)]
    print(f"Fetched {len(ids)} customers")
    return ids


@task(retries=2, retry_delay_seconds=5, tags=["processing", "customer"], name="process_customer_task", task_run_name="Process {date}_{id}_{n}")
def process_customer(id: str, n: int, date) -> str:
    """Process with simulated failures"""
    logger = get_run_logger()
    logger.info(f"Processing customer {id}_{n}...")
    if random.random() < 0.3:  # 30% processing failure
        raise RuntimeError(f"Processing failed: {id}_{n}")
    time.sleep(random.uniform(1, 4))
    result = f"Processed {id}_{n}"
    print(result)
    return result


@flow(timeout_seconds=30)
def pause(seconds: int):
    logger = get_run_logger()
    logger.info(f"Pausing for {seconds} seconds...")
    print(f"Pausing {seconds}s...")
    time.sleep(seconds)


@flow(name="main_pipeline", flow_run_name="Main Pipeline {date}")
def main_enhanced(date) -> List[str]:
    """Production-ready ETL demo"""
    logger = get_run_logger()
    logger.info("Starting enhanced pipeline...")

    customer_ids = get_customer_ids()
    pause(3) # Subflow pause

    # Parallel processing with retries
    results = process_customer.map(id=customer_ids, n=list(range(10)), date=date)

    print(f"Pipeline complete: {len(results)} results")
    return results


if __name__ == "__main__":
    main_enhanced(date)



# if __name__ == "__main__":
#     main_enhanced.serve(
#         name="my-first-deployment",
#         schedule=Cron(
#             "58 11 * * *",
#             timezone="Europe/Athens"
#         ))

