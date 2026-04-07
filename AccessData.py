import requests
import time
from pipeline import *

def get_json():
    names = fetch_pending_state_city_keys()

    print("TOTAL PENDING:", len(names))

    for state, city in names:

        payload = {
            'state': state,
            'city': city
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

                if data.get("data"):
                    parse_data(data, state, city)
                    update_state_city_status(state, city, "done")
                else:
                    update_state_city_status(state, city, "no_data")

            time.sleep(1)

        except Exception as e:
            print("ERROR:", e)


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

        print(obj)
        insert_dealer(obj)


def main():
    create_dealers_table()
    get_json()


if __name__ == "__main__":
    main()