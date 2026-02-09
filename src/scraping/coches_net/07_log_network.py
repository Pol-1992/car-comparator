from playwright.sync_api import sync_playwright

URL = "https://www.coches.net/segunda-mano/?pg=1"

KEYWORDS = ["search", "segunda", "covo", "ad", "listing", "api", "graphql", "vehicle", "vehic", "results"]

def main():
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        context = browser.contexts[0]

        # buscamos una pestaña de coches.net; si no, abrimos una
        page = None
        for pg in context.pages:
            if "coches.net" in (pg.url or ""):
                page = pg
                break
        if page is None:
            page = context.new_page()
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        print("Usando pestaña:", page.url)
        print("Recargando para capturar requests...")

        seen = set()

        def on_request(req):
            u = req.url
            ul = u.lower()
            if any(k in ul for k in KEYWORDS):
                if u not in seen:
                    seen.add(u)
                    print("REQ:", req.method, u)

        page.on("request", on_request)

        # recargamos y esperamos un poco
        page.reload(wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)

        print("\nTotal requests filtradas:", len(seen))
        browser.close()

if __name__ == "__main__":
    main()