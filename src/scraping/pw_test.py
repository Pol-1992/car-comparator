import asyncio
import sys
import re
from playwright.async_api import async_playwright

URL = "https://www.mobile.de/es/veh%C3%ADculos/detalles.html?id=423842448&sb=rel&od=up&vc=Car&cn=DE&ml=%3A175000&fr=2013&st=DEALER&pw=74&sr=4&dam=0&emc=EURO6&s=Car&searchId=0f1d3c77-c4c3-cc19-9b73-e53d57f96a2f&ref=srp&refId=0f1d3c77-c4c3-cc19-9b73-e53d57f96a2f"

def normalize_url(u: str) -> str:
    u = u.strip()
    if (u.startswith("'") and u.endswith("'")) or (u.startswith('"') and u.endswith('"')):
        u = u[1:-1].strip()
    return u

def first_line_matching(lines, pattern):
    rx = re.compile(pattern, re.IGNORECASE)
    for ln in lines:
        ln = ln.strip()
        if ln and rx.search(ln):
            return ln
    return None

def parse_int_from_text(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

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

async def extract_fields(url: str):
    url = normalize_url(url)
    print("0) URL:", url)

    async with async_playwright() as p:
        print("1) Launch Chromium")
        browser = await p.chromium.launch(headless=False)

        page = await browser.new_page()

        print("2) goto...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        print("3) wait 4s...")
        await page.wait_for_timeout(4000)

        print("4) read title + body text...")
        title = await page.title()
        body_text = await page.locator("body").inner_text()
        lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]

        print("5) find lines...")
        price_line = first_line_matching(lines, r"€")
        km_line = first_line_matching(lines, r"\b\d[\d\.\s]*\s?km\b")

        reg_line = first_line_matching(lines, r"\b(0?[1-9]|1[0-2])\s*/\s*(?:19|20)\d{2}\b")
        if not reg_line:
            reg_line = first_line_matching(lines, r"\b(?:19|20)\d{2}\b")

        first_reg, year = parse_first_registration(reg_line)

        result = {
            "url": url,
            "title": title,
            "price_eur": parse_int_from_text(price_line),
            "km": parse_int_from_text(km_line),
            "first_registration": first_reg,
            "year": year,
        }

        print("6) close browser")
        await browser.close()
        return result

async def main():
    data = await extract_fields(URL)
    print("\n=== RESULTADO LIMPIO ===")
    for k, v in data.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())