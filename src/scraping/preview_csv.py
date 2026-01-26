import csv
from pathlib import Path

CSV_PATH = Path("data/raw/mobile_de_results.csv")

with CSV_PATH.open("r", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

print("Filas:", len(rows))
print("Columnas:", rows[0].keys() if rows else "No hay filas")

# Mostrar primeras 5 filas
for r in rows[:5]:
    print("\n---")
    print("title:", r["title"])
    print("price_eur:", r["price_eur"], "| km:", r["km"], "| year:", r["year"])
    print("url:", r["url"][:80] + "...")