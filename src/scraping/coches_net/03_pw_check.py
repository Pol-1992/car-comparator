from playwright.sync_api import sync_playwright
import pathlib

URL = "https://www.coches.net/segunda-mano/?pg=1"

def main():
    user_data_dir = pathlib.Path("pw_profile_cochesnet")
    user_data_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        # Usamos un "persistent context" (perfil real con cookies/cache)
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
            viewport=None,
            locale="es-ES",
        )

        page = context.new_page()

        # Algunos headers "normales"
        page.set_extra_http_headers({
            "Accept-Language": "es-ES,es;q=0.9",
        })

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)

        print("TITLE:", page.title())
        print("Si aparece bloqueo/captcha, resolvelo en el navegador.")
        input("Cuando veas la página normal con anuncios, apretá ENTER aquí...")

        page.wait_for_timeout(2000)
        print("TITLE (post):", page.title())

        # Guardamos HTML
        html = page.content()
        with open("debug_pw_post.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("Guardado: debug_pw_post.html")

        # Conteo rápido
        body_text = page.inner_text("body")
        print("Conteo '€':", body_text.count("€"))

        context.close()

if __name__ == "__main__":
    main()