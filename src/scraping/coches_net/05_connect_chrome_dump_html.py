import pathlib
from playwright.sync_api import sync_playwright

TARGET_HOST = "coches.net"
FALLBACK_URL = "https://www.coches.net/segunda-mano/?pg=1"

def main():
    out = pathlib.Path("debug_connected_chrome.html")

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        context = browser.contexts[0]

        # 1) Buscar una pestaña que ya esté en coches.net
        target_page = None
        for pg in context.pages:
            try:
                if TARGET_HOST in (pg.url or ""):
                    target_page = pg
                    break
            except Exception:
                continue

        # 2) Si no existe, abrimos una nueva y vamos a la URL
        if target_page is None:
            target_page = context.new_page()
            target_page.goto(FALLBACK_URL, wait_until="domcontentloaded", timeout=60000)

        # 3) Asegurarnos de que esté cargada
        target_page.wait_for_timeout(2000)

        print("TITLE:", target_page.title())
        print("URL actual:", target_page.url)

        html = target_page.content()
        out.write_text(html, encoding="utf-8")
        print("Guardado:", out.resolve())

        browser.close()

if __name__ == "__main__":
    main()