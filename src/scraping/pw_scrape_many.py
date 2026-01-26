import asyncio
import sys
import re
import csv
import random
from pathlib import Path
from playwright.async_api import async_playwright

URLS_PATH = Path("src/scraping/urls.txt")
OUT_PATH = Path("data/raw/mobile_de_results.csv")

def normalize_url(u: str) -> str:
    u = u.strip().strip(" ,")
    if (u.startswith("'") and u.endswith("'")) or (u.startswith('"') and u.endswith('"')):
        u = u[1:-1].strip()
    return u

def first_line_matching(lines, pattern):
    rx = re.compile(pattern, re.IGNORECASE)
    for ln in lines:
        ln = ln.strip()
        if ln and rx.search(ln):
            return ln
    return None

def parse_int_from_text(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

def parse_first_registration(text):
    if not text:
        return None, None

    t = text.strip()

    m = re.search(r"(0?[1-9]|1[0-2])\s*/\s*((?:19|20)\d{2})", t)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        return f"{month:02d}/{year}", year

    m2 = re.search(r"\b((?:19|20)\d{2})\b", t)
    if m2:
        year = int(m2.group(1))
        return str(year), year

    return t, None

def price_from_title(title: str):
    if not title:
        return None
    m = re.search(r"para\s+([\d\.\s]+)\s*€", title, re.IGNORECASE)
    if not m:
        return None
    return parse_int_from_text(m.group(1))

async def goto_and_wait(page, url: str):
    # navegar sin networkidle (mobile.de nunca queda idle)
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

    # esperar a que el title tenga contenido (máx 15s)
    try:
        await page.wait_for_function("document.title && document.title.length > 3", timeout=15000)
    except Exception:
        pass

    # pequeña espera para que renderice
    await page.wait_for_timeout(2000)

async def scrape_one(page, url: str) -> dict:
    await goto_and_wait(page, url)

    title = (await page.title()) or ""

    # si está bloqueado, devolvemos marcador
    if "access denied" in title.lower() or "zugriff verweigert" in title.lower():
        return {
            "url": url,
            "title": title,
            "price_eur": None,
            "km": None,
            "first_registration": None,
            "year": None,
            "_blocked": True,
        }

    price_val = price_from_title(title)

    body_text = await page.locator("body").inner_text()
    lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]

    km_line = first_line_matching(lines, r"\b\d[\d\.\s]*\s?km\b")

    reg_line = first_line_matching(lines, r"\b(0?[1-9]|1[0-2])\s*/\s*(?:19|20)\d{2}\b")
    if not reg_line:
        reg_line = first_line_matching(lines, r"\b(?:19|20)\d{2}\b")

    first_reg, year = parse_first_registration(reg_line)

    return {
        "url": url,
        "title": title,
        "price_eur": price_val,
        "km": parse_int_from_text(km_line),
        "first_registration": first_reg,
        "year": year,
        "_blocked": False,
    }

async def main():
    if not URLS_PATH.exists():
        print(f"ERROR: No existe {URLS_PATH}.")
        return

    urls = [normalize_url(u) for u in URLS_PATH.read_text(encoding="utf-8").splitlines()]
    urls = [u for u in urls if u]

    print(f"Leí {len(urls)} URLs desde {URLS_PATH}")
    if not urls:
        print("ERROR: urls.txt está vacío.")
        return

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        context = await browser.new_context(
            locale="es-ES",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{len(urls)}] {url}")
            try:
                data = await scrape_one(page, url)

                # si vino bloqueado o title vacío, reintento 1 vez con pausa larga
                if data.get("_blocked") or (data.get("title", "").strip() == ""):
                    print("   -> Bloqueado o title vacío. Reintentando en 10s...")
                    await page.wait_for_timeout(10000)
                    data = await scrape_one(page, url)

                # quitamos el campo interno
                data.pop("_blocked", None)
                results.append(data)

                await page.wait_for_timeout(int(random.uniform(4000, 7000)))

            except Exception as e:
                print("   -> ERROR:", repr(e))
                results.append({
                    "url": url,
                    "title": None,
                    "price_eur": None,
                    "km": None,
                    "first_registration": None,
                    "year": None,
                })

        await context.close()
        await browser.close()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["url", "title", "price_eur", "km", "first_registration", "year"]
    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

    print(f"\nOK: guardado {len(results)} filas en {OUT_PATH}")

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())