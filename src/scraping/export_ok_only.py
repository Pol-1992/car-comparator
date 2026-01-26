import csv
from pathlib import Path

IN_PATH = Path("data/raw/mobile_de_results_all.csv")
OUT_PATH = Path("data/raw/mobile_de_results_ok_only.csv")

rows = []
with IN_PATH.open("r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for r in reader:
        if str(r.get("skipped", "")).lower() == "true":
            continue
        if str(r.get("blocked", "")).lower() == "true":
            continue
        rows.append(r)

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)

print("OK filas:", len(rows))
print("Guardado:", OUT_PATH)
