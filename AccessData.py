import requests
import time
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline import *

BACKUP_PATH = "BackupData_URL"

# folder create
os.makedirs(BACKUP_PATH, exist_ok=True)

MAX_THREADS = 5
BATCH_SIZE = 20


def save_backup(state, city, data):
    filename = f"{state}_{city}.json"
    filepath = os.path.join(BACKUP_PATH, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def hit_api(state, city):
    payload = {
        "state": state,
        "city": city
    }

    try:
        res = requests.post(
            "https://www.kia.com/api/kia2_in/findAdealer.getDealerList.do",
            data=payload,
            timeout=10
        )

        print("HIT:", state, city, res.status_code)

        if res.status_code == 200:
            data = res.json()

            save_backup(state, city, data)

            if data.get("data"):
                parse_data(data, state, city)
                update_state_city_status(state, city, "done")
            else:
                update_state_city_status(state, city, "no_data")

    except Exception as e:
        print("ERROR:", state, city, e)


def process_batch(batch):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(hit_api, s, c) for s, c in batch]

        for f in as_completed(futures):
            pass


def get_json():
    names = fetch_pending_state_city_keys()

    print("TOTAL PENDING:", len(names))

    # batching
    for i in range(0, len(names), BATCH_SIZE):
        batch = names[i:i + BATCH_SIZE]

        print(f"\nPROCESSING BATCH {i} to {i+len(batch)}")

        process_batch(batch)

        # small delay between batches
        time.sleep(2)


def parse_data(data, state, city):
    for i in data.get("data", []):

        obj = {
            "dealer_name": i.get("dealerName"),
            "dealer_id": i.get("id"),
            "dealer_type": i.get("dealerType"),
            "address": f"{i.get('address1','')} {i.get('address2','')}",
            "phone": i.get("phone1"),
            "email": i.get("email"),
            "web_url": i.get("website"),
            "state_name": i.get("stateName"),
            "city_name": i.get("cityName"),
            "state_key": state,
            "city_key": city,
            "source_url": f"https://www.kia.com/in/buy/find-a-dealer/result.html?state={state}&city={city}"
        }

        insert_dealer(obj)


def main():
    create_dealers_table()
    get_json()


if __name__ == "__main__":
    main()