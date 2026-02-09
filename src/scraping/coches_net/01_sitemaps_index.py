import requests
from bs4 import BeautifulSoup

SITEMAP_AD_SM_1 = "https://www.coches.net/servicios/sitemaps/sitemap-ad-sm-1.xml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://www.coches.net/",
    "Connection": "keep-alive",
}

def main():
    r = requests.get(SITEMAP_AD_SM_1, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]

    print(f"Total anuncios en este sitemap: {len(locs)}")
    print("Primeras 5 URLs:")
    for url in locs[:5]:
        print(" -", url)

if __name__ == "__main__":
    main()
