import os
import csv

INPUT_FILE = "data_gov_in.csv"   # your big CSV file
OUTPUT_DIR = "states_csv"
CHUNK_SIZE_MB = 500  # max size per file, set None if you don't want to split

def split_by_state(input_file, output_dir, chunk_size_mb=None):
    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames

        # open writers dynamically per state
        writers = {}
        files = {}
        sizes = {}
        counters = {}

        for row in reader:
            state = row.get("State", "UNKNOWN").replace(" ", "_")
            state_dir = os.path.join(output_dir, state)
            os.makedirs(state_dir, exist_ok=True)

            # initialize counters
            if state not in counters:
                counters[state] = 1
                sizes[state] = 0

            # determine filename
            filename = os.path.join(state_dir, f"{state}_part{counters[state]}.csv")

            # if writer not open for this state, open new one
            if state not in writers:
                f = open(filename, "w", newline="", encoding="utf-8")
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writers[state] = writer
                files[state] = f
                sizes[state] = len(",".join(headers).encode("utf-8"))

            # calculate row size
            line = ",".join([row[h] if row[h] is not None else "" for h in headers]) + "\n"
            row_size = len(line.encode("utf-8"))

            # if size exceeds chunk limit, close file and start new one
            if chunk_size_mb and sizes[state] + row_size > chunk_size_mb * 1024 * 1024:
                files[state].close()
                counters[state] += 1
                filename = os.path.join(state_dir, f"{state}_part{counters[state]}.csv")
                f = open(filename, "w", newline="", encoding="utf-8")
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writers[state] = writer
                files[state] = f
                sizes[state] = len(",".join(headers).encode("utf-8"))

            # write row
            writers[state].writerow(row)
            sizes[state] += row_size

        # close all files
        for f in files.values():
            f.close()

    print(f"✅ Split done! CSV files are organized in '{output_dir}'")

if __name__ == "__main__":
    split_by_state(INPUT_FILE, OUTPUT_DIR, CHUNK_SIZE_MB)
