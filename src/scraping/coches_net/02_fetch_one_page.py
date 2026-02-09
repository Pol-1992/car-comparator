import pathlib
import requests
from bs4 import BeautifulSoup

URL = "https://www.coches.net/segunda-mano/?pg=1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.1 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.coches.net/",
    "Connection": "keep-alive",
}

def main():
    resp = requests.get(URL, headers=HEADERS, timeout=30)

    print("Status:", resp.status_code)
    print("Content-Type:", resp.headers.get("Content-Type"))

    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else None
    print("TITLE:", title)

    # Guardamos HTML para inspección
    out_path = pathlib.Path("debug_cochesnet_segunda_mano_pg1.html")
    out_path.write_text(resp.text, encoding="utf-8")
    print("Guardado:", out_path.resolve())

    # Buscar "cards" como antes (varios selectores)
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
        if len(found) >= 5:
            candidates = found
            break

    if not candidates:
        candidates = best_pool

    print("Cards encontradas:", len(candidates))

    # Muestra rápida: 5 hrefs de las primeras cards
    print("Muestra 5 hrefs (primeras cards):")
    for c in candidates[:5]:
        a = c.select_one("a[href]")
        href = a.get("href") if a else None
        print(" -", href)

if __name__ == "__main__":
    main()