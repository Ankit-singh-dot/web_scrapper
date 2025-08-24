import requests
import concurrent.futures
import time
import random
import csv


MAX_WORKERS = 50
CHUNK_SIZE = 1000
URL = "https://complyrelax.com/Dashboard-CS/dininformation.php"

def fetch_din(din, session, retries=3):
    din_str = str(din).zfill(8)  # ✅ always 8 digit
    for attempt in range(retries):
        try:
            resp = session.post(
                URL,
                data={"din": din_str},
                timeout=30,
                verify=False
            )
            resp.raise_for_status()
            print(f"✅ Got DIN {din_str}")
            return {"din": din_str, "data": resp.text}

        except requests.exceptions.Timeout:
            wait = 2 ** attempt + random.random()
            print(f"⏳ Timeout DIN {din_str}, retry {attempt+1} in {wait:.1f}s...")
            time.sleep(wait)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error DIN {din_str}: {e}")
            return None

    return None



def run_scraper(start, end, output_file="din_results 80L, 91L .csv"):
    session = requests.Session()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk_start in range(start, end, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, end)
            chunk = range(chunk_start, chunk_end)

            futures = {
                executor.submit(fetch_din, din, session): din for din in chunk
            }

            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res:
                    results.append(res)


            if results:
                results.sort(key=lambda x: int(x["din"]))  # sort numerically
                with open(output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["din", "data"])
                    if f.tell() == 0:  # Write header if file empty
                        writer.writeheader()
                    writer.writerows(results)
                results = []  # reset after saving

            print(f"💾 Saved results till DIN {chunk_end}")

    print(" Scraping complete!")


if __name__ == "__main__":
    # Example: scrape 9800000 to 10000000
    run_scraper(8000000, 9100000)