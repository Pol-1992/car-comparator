import re
import csv
import time
import random
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

from playwright.sync_api import sync_playwright

# ===================== CONFIG =====================
HEADLESS = False
SLOW = True

OUT_CSV = Path("data/raw/mobile_de_FRESH.csv")
SEEN_URLS_TXT = Path("data/raw/mobile_de_seen_urls.txt")

MIN_YEAR = 2013
MAX_YEAR = 2025
MAX_PRICE = 30000
MAX_KM = 150000
MIN_KW = 110              # 110kW ≈ 150cv
EURO = "EURO6"
MIN_SELLER_STARS = 4
FUELS = ["PETROL", "DIESEL"]

BASE_URL = "https://www.mobile.de/es/veh%C3%ADculos/buscar.html"
BASE_PARAMS = {
    "isSearchRequest": "true",
    "s": "Car",
    "vc": "Car",
    "cn": "DE",
    "st": "DEALER",
    "sr": str(MIN_SELLER_STARS),
    "emc": EURO,
    "p": f":{MAX_PRICE}",
    "ml": f":{MAX_KM}",
    "pw": str(MIN_KW),
    "ref": "dsp",
}
# ===================== /CONFIG =====================

@dataclass
class SearchInfo:
    total_results: int
    max_page: int
    is_capped: bool

def rand_sleep(a=1.2, b=3.2):
    if SLOW:
        time.sleep(random.uniform(a, b))

def build_search_url(year_from: int, year_to: int) -> str:
    params = dict(BASE_PARAMS)
    params["fr"] = str(year_from)
    params["to"] = str(year_to)
    qs = [(k, v) for k, v in params.items()]
    for f in FUELS:
        qs.append(("ft", f))
    return f"{BASE_URL}?{urlencode(qs)}"

def set_page(url: str, page_num: int) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["pageNumber"] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def normalize_url(u: str) -> str:
    return (u or "").strip()

def extract_from_listing_text(text: str) -> dict:
    t = " ".join((text or "").split())

    m_price = re.search(r"(\d{1,3}(?:\.\d{3})*)\s*€", t)
    price = int(m_price.group(1).replace(".", "")) if m_price else None

    m_fr = re.search(r"\bPR\s*(0?[1-9]|1[0-2])/(20\d{2}|19\d{2})\b", t)
    first_reg = f"{int(m_fr.group(1)):02d}/{m_fr.group(2)}" if m_fr else None
    year = int(m_fr.group(2)) if m_fr else None

    m_km = re.search(r"(\d{1,3}(?:\.\d{3})*)\s*km\b", t, flags=re.I)
    km = int(m_km.group(1).replace(".", "")) if m_km else None

    m_kw_cv = re.search(r"(\d{2,3})\s*kW\s*\((\d{2,3})\s*cv\)", t, flags=re.I)
    kw = int(m_kw_cv.group(1)) if m_kw_cv else None
    cv = int(m_kw_cv.group(2)) if m_kw_cv else None
    if cv is None:
        m_cv = re.search(r"\b(\d{2,3})\s*cv\b", t, flags=re.I)
        cv = int(m_cv.group(1)) if m_cv else None
    if kw is None:
        m_kw = re.search(r"\b(\d{2,3})\s*kw\b", t, flags=re.I)
        kw = int(m_kw.group(1)) if m_kw else None
    if cv is None and kw is not None:
        cv = int(round(kw * 1.3596))

    fuel = None
    if re.search(r"\bGasolina\b", t, flags=re.I):
        fuel = "PETROL"
    elif re.search(r"\bDiesel\b|\bDi[eé]sel\b", t, flags=re.I):
        fuel = "DIESEL"

    m_rating = re.search(r"(\d(?:\.\d)?)\s*estrellas\s*\(\s*(\d+)\s*\)", t, flags=re.I)
    dealer_rating = float(m_rating.group(1)) if m_rating else None
    dealer_rating_count = int(m_rating.group(2)) if m_rating else None

    m_loc = re.search(r"\bDE-(\d{5})\s+([A-Za-zÄÖÜäöüß\-\s]+?)(?=\s+\d(?:\.\d)?\s*estrellas|\s*$)", t)
    location = f"{m_loc.group(1)} {m_loc.group(2).strip()}" if m_loc else None

    brand = None
    model = None
    if m_price:
        left = t[: m_price.start()].strip()
        left = re.sub(r"^(Patrocinado|NUEVO)\s+", "", left, flags=re.I).strip()
        toks = left.split()
        if toks:
            brand = toks[0]
            model = " ".join(toks[1:]) if len(toks) > 1 else None

    return {
        "title": t,
        "brand": brand,
        "model": model,
        "price_eur": price,
        "km": km,
        "kw": kw,
        "cv": cv,
        "fuel": fuel,
        "first_registration": first_reg,
        "year": year,
        "dealer_rating": dealer_rating,
        "dealer_rating_count": dealer_rating_count,
        "location": location,
    }

def accept_consent_if_needed(page):
    for txt in ["Aceptar", "Accept", "Rechazar", "Reject", "Einverstanden", "Alle akzeptieren", "Akzeptieren"]:
        btn = page.locator(f"button:has-text('{txt}')")
        if btn.count() > 0:
            try:
                btn.first.click(timeout=2000)
                page.wait_for_timeout(1200)
                break
            except:
                pass

def read_search_info(page) -> SearchInfo:
    body = page.inner_text("body")
    m_total = re.search(r"(\d{1,3}(?:\.\d{3})+)\s*(?:resultados|Ofertas)", body, flags=re.I)
    total = int(m_total.group(1).replace(".", "")) if m_total else -1

    max_page = -1
    pag = page.locator(
        "nav[aria-label*='Pagin'], nav[aria-label*='Pagination'], "
        "[data-testid*='pagination'], [class*='pagination']"
    ).first

    try:
        pag_text = pag.inner_text() if pag.count() > 0 else ""
    except:
        pag_text = ""

    m_pages = re.search(r"\b\d+\s*/\s*(\d+)\b", pag_text)
    if m_pages:
        max_page = int(m_pages.group(1))
    else:
        try:
            nums = page.locator("nav a, nav button, [data-testid*='pagination'] a, [data-testid*='pagination'] button")
            n = nums.count()
            found = []
            for i in range(min(n, 200)):
                tx = (nums.nth(i).inner_text() or "").strip()
                if re.fullmatch(r"\d{1,4}", tx):
                    found.append(int(tx))
            if found:
                max_page = max(found)
        except:
            pass

    is_capped = (max_page >= 50)
    return SearchInfo(total_results=total, max_page=max_page, is_capped=is_capped)

def get_listing_links(page):
    selectors = [
        "a[href*='detalles.html?id=']",
        "a[href*='details.html?id=']",
        "a[href*='detalles.html']",
        "a[href*='details.html']",
        "a[href*='/auto-inserat/']",
        "a[href*='?id=']",
    ]

    links = []
    for sel in selectors:
        loc = page.locator(sel)
        cnt = loc.count()
        if cnt == 0:
            continue
        for i in range(min(cnt, 400)):
            a = loc.nth(i)
            href = a.get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.mobile.de" + href
            href = normalize_url(href)
            if ("id=" not in href) and ("/auto-inserat/" not in href):
                continue

            aria = a.get_attribute("aria-label")
            text = aria if aria and len(aria) > 30 else (a.inner_text() or "")
            if len(text.strip()) < 30:
                try:
                    text = a.locator("xpath=ancestor::li[1]").inner_text()
                except:
                    pass

            links.append((href, text))

    ded = {}
    for u, t in links:
        ded[u] = t
    return [(u, ded[u]) for u in ded.keys()]

def ensure_csv(path: Path, fieldnames: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

def load_seen_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()}

def append_seen_urls(path: Path, urls: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        for u in urls:
            f.write(u + "\n")

def append_rows_csv(path: Path, fieldnames: list[str], rows: list[dict]):
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        for r in rows:
            w.writerow(r)

def split_year_ranges(page, start_year: int, end_year: int):
    ok_ranges = []
    stack = [(start_year, end_year)]

    while stack:
        y1, y2 = stack.pop()
        url = build_search_url(y1, y2)
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)
        accept_consent_if_needed(page)
        page.wait_for_timeout(1500)

        info = read_search_info(page)
        print(f"Rango {y1}-{y2}: total={info.total_results} max_page={info.max_page} capped={info.is_capped}")

        if info.is_capped and y1 < y2:
            mid = (y1 + y2) // 2
            stack.append((y1, mid))
            stack.append((mid + 1, y2))
        else:
            ok_ranges.append((y1, y2))

    ok_ranges.sort()
    return ok_ranges

def main():
    fieldnames = [
        "url","title","brand","model","price_eur","km","kw","cv","fuel",
        "first_registration","year","dealer_rating","dealer_rating_count","location",
        "year_from","year_to"
    ]
    ensure_csv(OUT_CSV, fieldnames)
    seen = load_seen_urls(SEEN_URLS_TXT)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            locale="de-DE",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            viewport={"width": 1280, "height": 850},
        )
        page = context.new_page()

        print("== Generando rangos de años para evitar cap de 50 páginas ==")
        ranges = split_year_ranges(page, MIN_YEAR, MAX_YEAR)
        print("Rangos finales:", ranges)

        total_saved = 0

        for (y1, y2) in ranges:
            base_url = build_search_url(y1, y2)

            # info + cap real
            page.goto(set_page(base_url, 1), wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            accept_consent_if_needed(page)
            page.wait_for_timeout(1500)

            info = read_search_info(page)
            max_pages = info.max_page if info.max_page and info.max_page > 0 else 1
            if max_pages > 50:
                max_pages = 50

            print(f"\n== Rango {y1}-{y2} | total={info.total_results} | pages={max_pages} ==")

            for pg in range(1, max_pages + 1):
                page_url = set_page(base_url, pg)
                page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1500)
                accept_consent_if_needed(page)
                rand_sleep()

                listings = get_listing_links(page)

                rows = []
                new_seen = []

                for ad_url, ad_text in listings:
                    if ad_url in seen:
                        continue

                    data = extract_from_listing_text(ad_text)
                    row = {"url": ad_url, **data, "year_from": y1, "year_to": y2}

                    if row["price_eur"] is None or row["km"] is None or row["cv"] is None or row["year"] is None:
                        continue
                    if row["price_eur"] > MAX_PRICE or row["km"] > MAX_KM or row["year"] < MIN_YEAR:
                        continue
                    if row["kw"] is not None and row["kw"] < MIN_KW:
                        continue
                    if row["fuel"] not in FUELS:
                        continue
                    if row["dealer_rating"] is not None and row["dealer_rating"] < MIN_SELLER_STARS:
                        continue

                    rows.append(row)
                    new_seen.append(ad_url)
                    seen.add(ad_url)

                if rows:
                    append_rows_csv(OUT_CSV, fieldnames, rows)
                    append_seen_urls(SEEN_URLS_TXT, new_seen)
                    total_saved += len(rows)

                print(f"  pág {pg:>2}/{max_pages}: listings={len(listings)} guardados={len(rows)} total_guardado={total_saved}")
                rand_sleep()

        context.close()
        browser.close()

    print(f"\n✅ Listo. Total guardado: {total_saved}")
    print(f"CSV: {OUT_CSV.resolve()}")

if __name__ == "__main__":
    main()
