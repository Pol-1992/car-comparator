import pathlib
import re
import pandas as pd
from bs4 import BeautifulSoup

HTML_PATH = pathlib.Path("debug_connected_chrome.html")

def safe_int(x: str | None):
    if not x:
        return None
    digits = re.sub(r"[^\d]", "", x)
    return int(digits) if digits else None

def main():
    if not HTML_PATH.exists():
        print("No existe:", HTML_PATH.resolve())
        print("Primero corré 05_connect_chrome_dump_html.py")
        return

    html = HTML_PATH.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    # Buscar cards (estrategia como en tu notebook)
    candidates = []
    selectors = [
        "article[data-testid*='card']",
        "div[data-testid*='card']",
        "div.mt-CardBasic",
        "li[data-testid*='ad']",
        "article",
    ]

    best_pool = []
    for css in selectors:
        found = [c for c in soup.select(css) if c.select_one("a[href]")]
        if len(found) > len(best_pool):
            best_pool = found
        if len(found) >= 10:
            candidates = found
            break
    if not candidates:
        candidates = best_pool

    print("Cards encontradas:", len(candidates))

    rows = []
    for c in candidates:
        a = c.select_one("a[href]")
        if not a:
            continue

        href = a.get("href")
        if not href:
            continue

        if href.startswith("http"):
            url = href
        elif href.startswith("/"):
            url = "https://www.coches.net" + href
        else:
            url = None

        title_el = c.select_one("h3") or c.select_one("h2") or a
        title = title_el.get_text(" ", strip=True) if title_el else None

        text_block = c.get_text(" ", strip=True)

        m_price = re.search(r"(\d[\d\.\s]*)\s*€", text_block)
        price = safe_int(m_price.group(1)) if m_price else None

        m_year = re.search(r"\b(19\d{2}|20\d{2})\b", text_block)
        year = int(m_year.group(1)) if m_year else None

        m_km = re.search(r"(\d[\d\.\s]*)\s*km\b", text_block, flags=re.I)
        km = safe_int(m_km.group(1)) if m_km else None

        m_cv = re.search(r"\b(\d{2,3})\s*cv\b", text_block, flags=re.I)
        cv = int(m_cv.group(1)) if m_cv else None

        # filtro mínimo anti-basura
        if not title or not price or not year or price < 2000:
            continue

        rows.append({
            "titulo": title,
            "url": url,
            "precio": price,
            "anio": year,
            "km": km,
            "cv": cv
        })

    df = pd.DataFrame(rows).drop_duplicates(subset=["url", "titulo"])
    print("Anuncios válidos:", len(df))
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()