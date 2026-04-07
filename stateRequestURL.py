import requests
import jmespath

from pipeline import create_kia_table, insert_kia_batch

TABLE_NAME = "kia_dealers"


def get_request():
    url = "https://www.kia.com/api/kia2_in/findAdealer.getStateCity.do"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("REQUEST FAILED:", e)
        return {}


def get_state_data(data):
    expression = """
    data.stateAndCity[].{
        state_name: val1.value,
        state_key: val1.key,
        cities: val2[].{
            city_name: value,
            city_key: key
        }
    }
    """

    try:
        return jmespath.search(expression, data) or []
    except Exception as e:
        print("JMESPATH ERROR:", e)
        return []


def prepare_data(states):
    final_rows = []

    for state in states:
        state_name = state.get("state_name")
        state_key = state.get("state_key")

        cities = state.get("cities") or []

        for city in cities:
            city_name = city.get("city_name")
            city_key = city.get("city_key")

            if not state_key or not city_key:
                continue

            url = f"https://www.kia.com/in/buy/find-a-dealer/result.html?state={state_key}&city={city_key}"

            final_rows.append({
                "state_name": state_name,
                "state_key": state_key,
                "city_name": city_name,
                "city_key": city_key,
                "url": url,
                "status": "pending"
            })

    return final_rows


def main():
    print("STARTING...")

    create_kia_table(TABLE_NAME)

    data = get_request()
    if not data:
        print("NO DATA FOUND")
        return

    states = get_state_data(data)
    if not states:
        print("NO STATES FOUND")
        return

    rows = prepare_data(states)
    if not rows:
        print("NO ROWS TO INSERT")
        return

    insert_kia_batch(TABLE_NAME, rows)

    print("TOTAL INSERTED:", len(rows))
    print("DONE")


if __name__ == "__main__":
    main()