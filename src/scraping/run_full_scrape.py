import asyncio
import csv
import os
import re
import time
from playwright.async_api import async_playwright, TimeoutError

# =========================
# CONFIG
# =========================

OUT_CSV = "data/raw/mobile_de_results_all.csv"
URLS_FILE = "src/scraping/urls_all.txt"

MAX_PAGES = 200          # seguridad
SLEEP_EVERY = 25         # pausa cada X anuncios
SLEEP_SECONDS = 20

YEAR_BLOCKS = [
    (2013, 2015),
    (2016, 2018),
    (2019, 2020),
    (2021, 2022),
    (2023, 2026),
]

# =========================
# HELPERS
# =========================

def build_search_url(fr, to, sr):
    return (
        "https://www.mobile.de/es/veh%C3%ADculos/buscar.html"
        "?isSearchRequest=true"
        "&s=Car"
        "&vc=Car"
        "&cn=DE"
        "&ml=%3A150000"      # km <= 150k
        "&p=%3A30000"        # precio <= 30k
        "&st=DEALER"         # concesionario
        "&sr=4"              # 4+ estrellas
        "&pw=74"             # >= 100cv
        "&dam=0"
        "&emc=EURO6"
        "&ref=dsp"
        f"&fr={fr}&to={to}"
        f"&srp={sr}"
    )

def extract_id(url):
    m = re.search(r"id=(\d+)", url)
    return m.group(1) if m else None

def read_existing_urls():
    if not os.path.exists(URLS_FILE):
        return set()
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        return set(l.strip() for l in f if l.strip())

def append_urls(new_urls):
    with open(URLS_FILE, "a", encoding="utf-8") as f:
        for u in new_urls:
            f.write(u + "\n")

def csv_exists():
    return os.path.exists(OUT_CSV)

def write_csv_row(row):
    write_header = not csv_exists()
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "url", "title", "brand", "model",
                "price_eur", "km",
                "first_registration", "year"
            ]
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)

# =========================
# SCRAPE DETAIL
# =========================

async def scrape_detail(page, url):
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(1200)
    except TimeoutError:
        return None

    text = await page.content()
    title = await page.title()

    # precio
    price = None
    m = re.search(r"(\d{1,3}(?:\.\d{3})*)\s*€", text)
    if m:
        price = int(m.group(1).replace(".", ""))

    # km
    km = None
    m = re.search(r"(\d{1,3}(?:\.\d{3})*)\s*km", text)
    if m:
        km = int(m.group(1).replace(".", ""))

    # matriculación
    first_reg = None
    year = None
    m = re.search(r"(\d{2}/\d{4})", text)
    if m:
        first_reg = m.group(1)
        year = int(first_reg.split("/")[1])

    # brand / model desde title
    brand = None
    model = None
    if title:
        parts = title.split(" para ")[0].split(" ")
        if len(parts) >= 2:
            brand = parts[0]
            model = " ".join(parts[1:])

    return {
        "url": url,
        "title": title,
        "brand": brand,
        "model": model,
        "price_eur": price,
        "km": km,
        "first_registration": first_reg,
        "year": year,
    }

# =========================
# MAIN
# =========================

async def main():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(URLS_FILE), exist_ok=True)

    seen_urls = read_existing_urls()
    seen_ids = {extract_id(u) for u in seen_urls}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print("\n=== PHASE 1: recolectando links ===")

        for fr, to in YEAR_BLOCKS:
            print(f"\n>>> BLOQUE {fr}-{to}")

            for page_idx in range(MAX_PAGES):
                sr = page_idx * 24 + 1
                search_url = build_search_url(fr, to, sr)

                try:
                    await page.goto(search_url, timeout=60000)
                    await page.wait_for_timeout(1500)
                except:
                    break

                links = await page.eval_on_selector_all(
                    "a[href*='detalles.html?id=']",
                    "els => els.map(e => e.href)"
                )

                new_links = []
                for l in links:
                    cid = extract_id(l)
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        seen_urls.add(l)
                        new_links.append(l)

                if not new_links:
                    break

                append_urls(new_links)
                print(f"  +{len(new_links)} links")

        print("\n=== PHASE 2: scraping anuncios ===")

        urls = list(seen_urls)
        count = 0

        for i, url in enumerate(urls, 1):
            row = await scrape_detail(page, url)
            if row:
                write_csv_row(row)
                count += 1

            if i % SLEEP_EVERY == 0:
                print("   -> descanso 20s...")
                time.sleep(SLEEP_SECONDS)

        await browser.close()

        print("\n=== FIN ===")
        print(f"URLs totales: {len(urls)}")
        print(f"Filas scrapeadas: {count}")
        print(f"CSV: {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())