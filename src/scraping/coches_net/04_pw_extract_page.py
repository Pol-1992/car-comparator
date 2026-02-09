import pathlib
from playwright.sync_api import sync_playwright

URL = "https://www.coches.net/segunda-mano/?pg=1"

def main():
    user_data_dir = pathlib.Path("pw_profile_cochesnet")
    user_data_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            args=["--start-maximized", "--disable-blink-features=AutomationControlled"],
            viewport=None,
            locale="es-ES",
        )

        page = context.new_page()
        page.set_extra_http_headers({"Accept-Language": "es-ES,es;q=0.9"})

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        print("TITLE:", page.title())
        print("Ahora NO hagas nada automático.")
        print("Durante 10 segundos: mové el mouse, scrolleá un poquito MANUAL en el navegador si querés.")
        page.wait_for_timeout(10000)

        html = page.content()
        out_path = pathlib.Path("debug_pw_minimal.html")
        out_path.write_text(html, encoding="utf-8")
        print("Guardado:", out_path.resolve())

        print("Listo. Cerrando.")
        context.close()

if __name__ == "__main__":
    main()