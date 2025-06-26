import csv
import json
import time
import requests
from pathlib import Path

# === CONFIGURATION ===
AUTHOR_CSV = "persistent_authors_gnd.csv"
OUTPUT_IDS_FILE = "gnd_ids.txt"
OUTPUT_JSON_FILE = "gnd_dump.json"
OUTPUT_CSV_FILE = "gnd_dump.csv"
DELAY_SECONDS = 0.2
BASE_URL = "https://lobid.org/gnd/{}.json"


class GndIdExtractor:
    def __init__(self, author_csv_path):
        self.author_csv_path = author_csv_path

    def extract_ids(self):
        unique_ids = set()
        with open(self.author_csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for key in ['gnd_id', 'gnd_id_2']:
                    val = row.get(key, '').strip()
                    if val:
                        unique_ids.add(val)
        return sorted(unique_ids)

    def write_ids(self, ids, output_path):
        Path(output_path).write_text("\n".join(ids), encoding="utf-8")


class GndFetcher:
    def __init__(self, gnd_ids, base_url=BASE_URL, delay=DELAY_SECONDS):
        self.gnd_ids = gnd_ids
        self.base_url = base_url
        self.delay = delay
        self.records = []

    def _get_list_field(self, data, key):
        value = data.get(key, [])
        if isinstance(value, list):
            return [v["label"] if isinstance(v, dict) and "label" in v else str(v) for v in value]
        elif isinstance(value, dict) and "label" in value:
            return [value["label"]]
        elif value:
            return [str(value)]
        return []

    def fetch_all(self):
        for index, gnd_id in enumerate(self.gnd_ids, start=1):
            print(f"[{index}/{len(self.gnd_ids)}] Fetching {gnd_id}...", end="\r")
            url = self.base_url.format(gnd_id)
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                record = {
                    "gnd_id": data.get("gndIdentifier"),
                    "preferred_name": data.get("preferredName"),
                    "variant_names": self._get_list_field(data, "variantName"),
                    "date_of_birth": data.get("dateOfBirth"),
                    "date_of_death": data.get("dateOfDeath"),
                    "professions": self._get_list_field(data, "professionOrOccupation"),
                    "places_of_birth": self._get_list_field(data, "placeOfBirth"),
                }
                self.records.append(record)
                time.sleep(self.delay)
            except Exception as e:
                print(f"Error fetching {gnd_id}: {e}")

    def write_json(self, path):
        Path(path).write_text(json.dumps(self.records, indent=2, ensure_ascii=False), encoding="utf-8")

    def flip_name(self, name):
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            return f"{parts[1]} {parts[0]}"
        return name.strip()

    def list_to_str(self, val):
        if isinstance(val, list):
            return "|".join(str(v) for v in val if v)
        elif val:
            return str(val)
        return ""

    def write_csv(self, path):
        with open("gnd_dump.csv", "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "id",
                "preferred_name",
                "variant_names",
                "date_of_birth",
                "date_of_death",
                "professions",
                "places_of_birth"
            ])
            writer.writeheader()

            for rec in self.records:
                # Flip all variant names
                flipped_variants = [self.flip_name(v) for v in rec["variant_names"] if v]

                # Add flipped preferred name as well
                if rec["preferred_name"]:
                    flipped_variants.append(self.flip_name(rec["preferred_name"]))

                writer.writerow({
                    "id": rec["gnd_id"],
                    "preferred_name": rec["preferred_name"],
                    "variant_names": "|".join(flipped_variants),
                    "date_of_birth": self.list_to_str(rec["date_of_birth"]),
                    "date_of_death": self.list_to_str(rec["date_of_death"]),
                    "professions": self.list_to_str(rec["professions"]),
                    "places_of_birth": self.list_to_str(rec["places_of_birth"]),
                })


if __name__ == "__main__":
    extractor = GndIdExtractor(AUTHOR_CSV)
    gnd_ids = extractor.extract_ids()
    extractor.write_ids(gnd_ids, OUTPUT_IDS_FILE)

    fetcher = GndFetcher(gnd_ids)
    fetcher.fetch_all()
    fetcher.write_json(OUTPUT_JSON_FILE)
    fetcher.write_csv(OUTPUT_CSV_FILE)
    print('done')