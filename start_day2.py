from prefect import flow, task
import random
import time
from typing import List

from prefect.schedules import Cron


@task(retries=3, retry_delay_seconds=5)
def get_customer_ids() -> List[str]:
    """Fetch with retry logic"""
    if random.random() < 0.1:  # 10% fetch failure
        raise ValueError("API down")
    ids = [f"customer{random.randint(1, 100)}" for _ in range(10)]
    print(f"Fetched {len(ids)} customers")
    return ids


@task(retries=2, retry_delay_seconds=3)
def process_customer(customer_id: str, n: int) -> str:
    """Process with simulated failures"""
    if random.random() < 0.3:  # 30% processing failure
        raise RuntimeError(f"Processing failed: {customer_id}_{n}")
    time.sleep(random.uniform(1, 4))
    result = f"Processed {customer_id}_{n}"
    print(result)
    return result


@flow
def pause(seconds: int):
    print(f"Pausing {seconds}s...")
    time.sleep(seconds)


@flow
def main_enhanced() -> List[str]:
    """Production-ready ETL demo"""
    print("Starting enhanced pipeline...")

    customer_ids = get_customer_ids()
    pause(3) # Subflow pause

    # Parallel processing with retries
    results = process_customer.map(customer_ids, list(range(10)))

    print(f"Pipeline complete: {len(results)} results")
    return results


# if __name__ == "__main__":
#     main_enhanced()


# if __name__ == "__main__":
#     main_enhanced.serve(
#         name="my-first-deployment",
#         cron="30 10 * * *",  # Run every day at 10:30 AM
#     )

if __name__ == "__main__":
    main_enhanced.serve(
        name="my-first-deployment",
        schedule=Cron(
            "51 10 * * *",
            timezone="Europe/Athens"
        ))

