import requests
import csv
import time

# ===== CONFIGURATION =====
API_KEY = "579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645"
RESOURCE_ID = "8b68ae56-84cf-4728-a0a6-1be11028dea7"

BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"       # For records
CURL_URL = f"https://www.data.gov.in/backend/dataapi/v1/resource/{RESOURCE_ID}"  # For state/district lists

OUTPUT_FILE = "data_gov_in.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:142.0) Gecko/20100101 Firefox/142.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.data.gov.in/resource/list-msme-registered-units-under-udyam",
}

# ===== GET ALL STATES =====
def get_all_states():
    params = {
        "format": "json",
        "api-key": API_KEY,
        "aggr[State][terms][field]": "State",
        "aggr[State][terms][size]": "1000",
        "aggr_show": "1"
    }
    r = requests.get(CURL_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    if "records" in data:
        return sorted({rec["State"] for rec in data["records"] if "State" in rec})
    return []


def get_districts_for_state(state):
    params = {
        "format": "json",
        "api-key": API_KEY,
        "filters[State]": state,
        "aggr[District][terms][field]": "District",
        "aggr[District][terms][size]": "1000",
        "aggr_show": "1"
    }
    r = requests.get(CURL_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    if "records" in data:
        return sorted({rec["District"] for rec in data["records"] if "District" in rec})
    return []

# ===== FETCH ALL RECORDS FOR A STATE-DISTRICT WITH PAGINATION =====
def fetch_all_data(state, district):
    all_data = []
    offset = 0
    limit = 10000  # Max records per page

    while True:
        params = {
            "format": "json",
            "api-key": API_KEY,
            "filters[State]": state,
            "filters[District]": district,
            "limit": limit,
            "offset": offset
        }
        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        data = r.json()
        records = data.get("records", [])
        if not records:
            break
        all_data.extend(records)
        offset += limit
        print(f"   ➡ Retrieved {len(records)} records (total {len(all_data)}) for {district}")
    return all_data

# ===== MAIN SCRIPT =====
def main():
    all_records = []
    states = get_all_states()
    print(f"🌍 Found {len(states)} states")

    for state in states:
        districts = get_districts_for_state(state)
        print(f"📌 {state}: {len(districts)} districts")

        for district in districts:
            print(f"📡 Fetching {state} - {district}")
            try:
                records = fetch_all_data(state, district)
                for record in records:
                    record["State"] = state
                    record["District"] = district
                    all_records.append(record)
            except Exception as e:
                print(f"❌ Error fetching {state} - {district}: {e}")

            time.sleep(1)  # Avoid rate limiting

    if all_records:
        keys = list(all_records[0].keys())
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_records)
        print(f"✅ Data saved to {OUTPUT_FILE}")
    else:
        print("⚠ No data fetched.")

if __name__ == "__main__":
    main()
