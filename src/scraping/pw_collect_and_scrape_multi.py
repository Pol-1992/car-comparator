import asyncio
import sys
import re
import csv
import random
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright

MAX_PAGES = 200
MAX_LINKS = 20000
HEADLESS = False
SLOW_MODE = True

SEARCH_LIST = Path("src/scraping/search_urls.txt")
URLS_OUT = Path("src/scraping/urls_all.txt")
CSV_OUT = Path("data/raw/mobile_de_results_all.csv")

# ====== REGLAS DURAS ======
MIN_YEAR = 2013
MAX_KM = 150_000
MAX_PRICE = 30_000

# ---------------- URL helpers ----------------
def normalize_url(u: str) -> str:
    u = (u or "").strip().strip(" ,")
    if (u.startswith("'") and u.endswith("'")) or (u.startswith('"') and u.endswith('"')):
        u = u[1:-1].strip()
    return u

def canonical_vehicle_url(u: str) -> str:
    u = normalize_url(u)
    parts = urlparse(u)
    q = parse_qs(parts.query)
    ad_id = q.get("id", [None])[0]
    if not ad_id:
        return u
    return f"{parts.scheme}://{parts.netloc}{parts.path}?id={ad_id}"

def extract_id(u: str) -> str | None:
    u = normalize_url(u)
    parts = urlparse(u)
    q = parse_qs(parts.query)
    return q.get("id", [None])[0]

# ---------------- parsing helpers ----------------
def parse_int_from_text(text: str | None):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

def first_line_matching(lines, pattern):
    rx = re.compile(pattern, re.IGNORECASE)
    for ln in lines:
        ln = ln.strip()
        if ln and rx.search(ln):
            return ln
    return None

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

def brand_model_from_title(title: str):
    if not title:
        return None, None
    parts = re.split(r"\s+para\s+", title, flags=re.IGNORECASE, maxsplit=1)
    left = parts[0].strip() if parts else title.strip()
    if not left:
        return None, None
    tokens = left.split()
    if len(tokens) == 1:
        return tokens[0], None
    brand = tokens[0].strip()
    model = " ".join(tokens[1:]).strip()
    return brand, model

def apply_hard_rules(price_eur, km, year):
    reasons = []
    if year is None or year < MIN_YEAR:
        reasons.append(f"year<{MIN_YEAR}")
    if km is None or km > MAX_KM:
        reasons.append(f"km>{MAX_KM}")
    if price_eur is None or price_eur > MAX_PRICE:
        reasons.append(f"price>{MAX_PRICE}")
    if reasons:
        return True, "|".join(reasons)
    return False, ""

# ---------------- IO helpers ----------------
def load_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]

def load_existing_urls(path: Path) -> set[str]:
    return {canonical_vehicle_url(x) for x in load_lines(path) if canonical_vehicle_url(x)}

def append_urls(path: Path, new_urls: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        for u in new_urls:
            f.write(u + "\n")

def ensure_csv_header(path: Path, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

def append_csv_row(path: Path, fieldnames: list[str], row: dict) -> None:
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)

def load_scraped_ids_from_csv(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out = set()
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = row.get("url", "")
            ad_id = extract_id(u) if u else None
            if ad_id:
                out.add(ad_id)
    return out

# ---------------- Playwright helpers ----------------
async def human_pause(min_ms=2500, max_ms=5000):
    if not SLOW_MODE:
        return
    await asyncio.sleep(random.uniform(min_ms/1000, max_ms/1000))

async def make_page(p):
    browser = await p.chromium.launch(
        headless=HEADLESS,
        args=["--disable-dev-shm-usage", "--no-sandbox"],
    )
    context = await browser.new_context(
        locale="es-ES",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )
    page = await context.new_page()
    return browser, context, page

async def safe_goto(p, browser, context, page, url: str):
    for attempt in range(1, 3):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1500)
            try:
                await page.wait_for_function("document.title && document.title.length > 3", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(800)
            return browser, context, page
        except Exception as e:
            msg = repr(e)
            if "TargetClosedError" in msg or "has been closed" in msg:
                print(f"   -> TargetClosedError navegando (attempt {attempt}). Recreo browser/context/page...")
                try:
                    try:
                        await context.close()
                    except Exception:
                        pass
                    try:
                        await browser.close()
                    except Exception:
                        pass
                finally:
                    browser, context, page = await make_page(p)
                await page.wait_for_timeout(1500)
                continue

            if attempt < 2:
                print(f"   -> Error navegando (attempt {attempt}): {msg}\n      Reintento en 5s...")
                await page.wait_for_timeout(5000)
                continue
            raise
    return browser, context, page

async def safe_get_title(page):
    """
    Evita 'Execution context was destroyed' por navegación.
    """
    for attempt in range(1, 4):
        try:
            # Esperar un poco a que se estabilice la navegación
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception:
                pass

            # Leer title por DOM (más robusto)
            loc = page.locator("title")
            if await loc.count() > 0:
                t = (await loc.first.inner_text()) or ""
                if t.strip():
                    return t.strip()

            # fallback: API title
            t2 = (await page.title()) or ""
            return t2.strip()

        except Exception as e:
            msg = repr(e)
            if "Execution context was destroyed" in msg:
                await page.wait_for_timeout(1200)
                continue
            if attempt < 3:
                await page.wait_for_timeout(800)
                continue
            return ""

    return ""

async def collect_links_from_results(page) -> list[str]:
    links = await page.eval_on_selector_all(
        "a[href*='detalles.html?id=']",
        "els => Array.from(new Set(els.map(e => e.href)))"
    )
    out = []
    for u in links:
        u = canonical_vehicle_url(u)
        if "mobile.de" in u and "detalles.html" in u and re.search(r"id=\d+", u):
            out.append(u)
    return sorted(set(out))

async def go_next_page(page) -> bool:
    selectors = [
        "a[rel='next']",
        "a:has-text('Siguiente')",
        "a:has-text('Weiter')",
        "a:has-text('Next')",
        "[aria-label*='Siguiente']",
        "[aria-label*='Weiter']",
        "[data-testid*='next'] a",
    ]
    for sel in selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            try:
                await loc.first.scroll_into_view_if_needed()
                await loc.first.click()
                await page.wait_for_timeout(2000)
                return True
            except Exception:
                pass
    return False

async def scrape_one(p, browser, context, page, url: str):
    browser, context, page = await safe_goto(p, browser, context, page, url)

    title = await safe_get_title(page)

    blocked = ("access denied" in title.lower()) or ("zugriff verweigert" in title.lower())
    if blocked:
        return {
            "url": url,
            "title": title,
            "brand": None,
            "model": None,
            "price_eur": None,
            "km": None,
            "first_registration": None,
            "year": None,
            "blocked": True,
            "skipped": True,
            "skip_reason": "blocked",
        }, browser, context, page

    brand, model = brand_model_from_title(title)
    price_val = price_from_title(title)

    body_text = await page.locator("body").inner_text()
    lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]

    km_line = first_line_matching(lines, r"\b\d[\d\.\s]*\s?km\b")
    km_val = parse_int_from_text(km_line)

    reg_line = first_line_matching(lines, r"\b(0?[1-9]|1[0-2])\s*/\s*(?:19|20)\d{2}\b")
    if not reg_line:
        reg_line = first_line_matching(lines, r"\b(?:19|20)\d{2}\b")

    first_reg, year_val = parse_first_registration(reg_line)

    skipped, reason = apply_hard_rules(price_val, km_val, year_val)

    return {
        "url": url,
        "title": title,
        "brand": brand,
        "model": model,
        "price_eur": price_val,
        "km": km_val,
        "first_registration": first_reg,
        "year": year_val,
        "blocked": False,
        "skipped": skipped,
        "skip_reason": reason,
    }, browser, context, page

async def main():
    if not SEARCH_LIST.exists():
        print("ERROR: no existe src/scraping/search_urls.txt")
        return

    searches = load_lines(SEARCH_LIST)
    print("Búsquedas:", len(searches))

    fieldnames = [
        "url", "title", "brand", "model",
        "price_eur", "km", "first_registration", "year",
        "blocked", "skipped", "skip_reason"
    ]
    ensure_csv_header(CSV_OUT, fieldnames)

    known_urls = load_existing_urls(URLS_OUT)
    scraped_ids = load_scraped_ids_from_csv(CSV_OUT)
    print("URLs ya guardadas:", len(known_urls))
    print("IDs ya scrapeados:", len(scraped_ids))

    async with async_playwright() as p:
        browser, context, page = await make_page(p)

        print("\n=== PHASE 1: collect multi-search ===")
        for si, s_url in enumerate(searches, start=1):
            print(f"\n[SEARCH {si}/{len(searches)}] {s_url}")
            browser, context, page = await safe_goto(p, browser, context, page, s_url)

            for pi in range(1, MAX_PAGES + 1):
                links = await collect_links_from_results(page)
                new_links = [u for u in links if u not in known_urls]

                if new_links:
                    known_urls.update(new_links)
                    append_urls(URLS_OUT, new_links)
                    print(f"  [page {pi}] +{len(new_links)} links | total={len(known_urls)}")
                else:
                    print(f"  [page {pi}] 0 nuevos")

                if len(known_urls) >= MAX_LINKS:
                    print("  Alcancé MAX_LINKS. Corto.")
                    break

                ok = await go_next_page(page)
                await human_pause()
                if not ok:
                    print("  No hay 'Siguiente'. Fin de esta búsqueda.")
                    break

        print("\n=== PHASE 2: scrape pendientes ===")
        all_urls = sorted(load_existing_urls(URLS_OUT))[:MAX_LINKS]
        to_scrape = []
        for u in all_urls:
            ad_id = extract_id(u)
            if ad_id and ad_id not in scraped_ids:
                to_scrape.append(u)

        print(f"URLs totales: {len(all_urls)} | pendientes: {len(to_scrape)}")

        blocked = 0
        scraped_now = 0
        skipped_now = 0

        for i, url in enumerate(to_scrape, start=1):
            print(f"[{i}/{len(to_scrape)}] {url}")

            row, browser, context, page = await scrape_one(p, browser, context, page, url)

            # reintento suave si title vacío
            if not row.get("title"):
                print("   -> title vacío. Reintento en 6s...")
                await page.wait_for_timeout(6000)
                row, browser, context, page = await scrape_one(p, browser, context, page, url)

            if row.get("blocked"):
                blocked += 1
            if row.get("skipped"):
                skipped_now += 1

            append_csv_row(CSV_OUT, fieldnames, row)

            ad_id = extract_id(row.get("url", ""))
            if ad_id:
                scraped_ids.add(ad_id)

            scraped_now += 1
            await human_pause(3500, 7500)
            if scraped_now % 25 == 0:
                print("   -> descanso 20s...")
                await page.wait_for_timeout(20000)

        try:
            await context.close()
        except Exception:
            pass
        try:
            await browser.close()
        except Exception:
            pass

    print("\n=== FIN ===")
    print("URLs:", len(load_existing_urls(URLS_OUT)))
    print("Scrapeadas esta corrida:", scraped_now)
    print("Bloqueadas:", blocked)
    print("Skipped (fuera de reglas):", skipped_now)
    print("CSV:", CSV_OUT)

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())

