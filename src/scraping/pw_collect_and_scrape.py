import asyncio
import sys
import re
import csv
import random
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright

# =========================
# CONFIG
# =========================
SEARCH_URL = "https://www.mobile.de/es/veh%C3%ADculos/buscar.html?isSearchRequest=true&s=Car&vc=Car&cn=DE&ml=%3A175000&fr=2013&st=DEALER&pw=74&sr=4&dam=0&emc=EURO6&ref=dsp"

MAX_PAGES = 200
MAX_LINKS = 5000
HEADLESS = False
SLOW_MODE = True

URLS_OUT = Path("src/scraping/urls_all.txt")
CSV_OUT = Path("data/raw/mobile_de_results_all.csv")

# =========================
# Helpers: URL & parsing
# =========================
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

# =========================
# Helpers: IO
# =========================
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

# =========================
# Helpers: Browser behavior
# =========================
async def human_pause(min_ms=2500, max_ms=5000):
    if not SLOW_MODE:
        return
    await asyncio.sleep(random.uniform(min_ms/1000, max_ms/1000))

async def make_page(p):
    """
    Crea browser/context/page nuevos.
    """
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

async def safe_goto_soft(p, browser, context, page, url: str):
    """
    Navega con recovery si el page/context/browser se cerró.
    Devuelve (browser, context, page) por si recrea.
    """
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
            # otro error: reintento simple
            if attempt < 2:
                print(f"   -> Error navegando (attempt {attempt}): {msg}\n      Reintento en 5s...")
                await page.wait_for_timeout(5000)
                continue
            raise
    return browser, context, page

# =========================
# Phase 1: Collect links
# =========================
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

# =========================
# Phase 2: Scrape details
# =========================
async def scrape_one(p, browser, context, page, url: str) -> tuple[dict, object, object, object]:
    """
    Devuelve (row, browser, context, page) porque puede recrearlos.
    """
    browser, context, page = await safe_goto_soft(p, browser, context, page, url)
    title = (await page.title()) or ""

    blocked = ("access denied" in title.lower()) or ("zugriff verweigert" in title.lower())
    if blocked:
        return ({
            "url": url,
            "title": title,
            "price_eur": None,
            "km": None,
            "first_registration": None,
            "year": None,
            "blocked": True,
        }, browser, context, page)

    price_val = price_from_title(title)

    body_text = await page.locator("body").inner_text()
    lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]

    km_line = first_line_matching(lines, r"\b\d[\d\.\s]*\s?km\b")

    reg_line = first_line_matching(lines, r"\b(0?[1-9]|1[0-2])\s*/\s*(?:19|20)\d{2}\b")
    if not reg_line:
        reg_line = first_line_matching(lines, r"\b(?:19|20)\d{2}\b")

    first_reg, year = parse_first_registration(reg_line)

    return ({
        "url": url,
        "title": title,
        "price_eur": price_val,
        "km": parse_int_from_text(km_line),
        "first_registration": first_reg,
        "year": year,
        "blocked": False,
    }, browser, context, page)

# =========================
# Main
# =========================
async def main():
    fieldnames = ["url", "title", "price_eur", "km", "first_registration", "year", "blocked"]
    ensure_csv_header(CSV_OUT, fieldnames)

    known_urls = load_existing_urls(URLS_OUT)
    scraped_ids = load_scraped_ids_from_csv(CSV_OUT)

    print(f"URLs ya guardadas (dedup por id): {len(known_urls)}")
    print(f"IDs ya scrapeados (desde CSV): {len(scraped_ids)}")

    async with async_playwright() as p:
        browser, context, page = await make_page(p)

        # =========================
        # PHASE 1: Collect
        # =========================
        print("\n=== PHASE 1: recolectando links (DOM next) ===")

        browser, context, page = await safe_goto_soft(p, browser, context, page, SEARCH_URL)

        pages_done = 0
        while pages_done < MAX_PAGES and len(known_urls) < MAX_LINKS:
            pages_done += 1

            links = await collect_links_from_results(page)
            new_links = [u for u in links if u not in known_urls]

            if new_links:
                known_urls.update(new_links)
                append_urls(URLS_OUT, new_links)
                print(f"[page {pages_done}] +{len(new_links)} links | total={len(known_urls)}")
            else:
                print(f"[page {pages_done}] 0 links nuevos")

            ok = await go_next_page(page)
            await human_pause()
            if not ok:
                print("No encontré 'Siguiente'. Fin paginación.")
                break

        # =========================
        # PHASE 2: Scrape (resume + recovery)
        # =========================
        print("\n=== PHASE 2: scraping anuncios (reanuda + recovery) ===")

        all_urls = sorted(load_existing_urls(URLS_OUT))[:MAX_LINKS]
        to_scrape = []
        for u in all_urls:
            ad_id = extract_id(u)
            if ad_id and ad_id not in scraped_ids:
                to_scrape.append(u)

        print(f"URLs totales: {len(all_urls)} | pendientes (no scrapeadas): {len(to_scrape)}")

        scraped_now = 0
        blocked_count = 0

        for i, url in enumerate(to_scrape, start=1):
            print(f"[{i}/{len(to_scrape)}] {url}")

            success = False
            for attempt in range(1, 3):
                try:
                    row, browser, context, page = await scrape_one(p, browser, context, page, url)

                    if row.get("blocked") or not row.get("title"):
                        print(f"   -> bloqueado/title vacío (attempt {attempt}). Espero 12s y reintento...")
                        await page.wait_for_timeout(12000)
                        row, browser, context, page = await scrape_one(p, browser, context, page, url)

                    if row.get("blocked"):
                        blocked_count += 1

                    append_csv_row(CSV_OUT, fieldnames, row)
                    ad_id = extract_id(row.get("url", ""))
                    if ad_id:
                        scraped_ids.add(ad_id)

                    scraped_now += 1
                    success = True

                    await human_pause(4000, 8000)
                    if scraped_now % 25 == 0:
                        print("   -> descanso 20s...")
                        await page.wait_for_timeout(20000)

                    break

                except Exception as e:
                    msg = repr(e)
                    if attempt < 2:
                        print(f"   -> Error (attempt {attempt}): {msg}\n      Reintento en 8s...")
                        await page.wait_for_timeout(8000)
                        continue

            if not success:
                print("   -> FALLÓ 2 veces. Guardo fila vacía y sigo.")
                append_csv_row(CSV_OUT, fieldnames, {
                    "url": url,
                    "title": None,
                    "price_eur": None,
                    "km": None,
                    "first_registration": None,
                    "year": None,
                    "blocked": None,
                })

        # cerrar al final
        try:
            await context.close()
        except Exception:
            pass
        try:
            await browser.close()
        except Exception:
            pass

    print("\n=== FIN ===")
    print(f"Páginas visitadas: {pages_done}")
    print(f"URLs guardadas (dedup): {len(load_existing_urls(URLS_OUT))}")
    print(f"Scrapeadas en esta corrida: {scraped_now}")
    print(f"Bloqueadas: {blocked_count}")
    print(f"URLs file: {URLS_OUT}")
    print(f"CSV file: {CSV_OUT}")

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())