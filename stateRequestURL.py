import requests
import jmespath

from pipeline import create_state_city_table, insert_state_city_batch


def get_data():
    url = "https://www.kia.com/api/kia2_in/findAdealer.getStateCity.do"
    return requests.get(url).json()


def parse(data):
    exp = """
    data.stateAndCity[].{
        state_name: val1.value,
        state_key: val1.key,
        cities: val2[].{
            city_name: value,
            city_key: key
        }
    }
    """
    return jmespath.search(exp, data)


def prepare(states):
    rows = []

    for s in states:
        for c in s["cities"]:
            rows.append({
                "state_name": s["state_name"],
                "state_key": s["state_key"],
                "city_name": c["city_name"],
                "city_key": c["city_key"]
            })

    return rows


def main():
    print("STARTING...")

    create_state_city_table()

    data = get_data()
    states = parse(data)
    rows = prepare(states)

    insert_state_city_batch(rows)

    print("TOTAL INSERTED:", len(rows))
    print("DONE")


if __name__ == "__main__":
    main()