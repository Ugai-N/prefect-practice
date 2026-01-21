from prefect import flow, task
import random
import time

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    ids = [f"customer{n}" for n in random.choices(range(100), k=10)]
    print(ids)
    return ids

@task
def process_customer(customer_id: str, n) -> str:
    # Process a single customer
    time.sleep(4)
    print(f"Processed {customer_id}_{n}")
    return f"Processed {customer_id}_{n}"

@flow
def pause(seconds: int) -> None:
    print(f"Pausing for {seconds} seconds...")
    time.sleep(seconds)

@flow
def main2() -> list[str]:
    customer_ids = get_customer_ids()
    numb = [n for n in range(10)]
    pause(10)
    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids, numb)
    return results


if __name__ == "__main__":
    main2()