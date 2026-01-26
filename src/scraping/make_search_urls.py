from pathlib import Path

# Reglas del proyecto:
# - año >= 2013
# - potencia >= ~100cv (pw=74 kW)
# - km <= 150.000
# - precio <= 30.000
# - solo concesionario
# - dealer rating >= 4
# - EURO6
base = (
    "https://www.mobile.de/es/veh%C3%ADculos/buscar.html?"
    "isSearchRequest=true"
    "&s=Car"
    "&vc=Car"
    "&cn=DE"
    "&ml=%3A150000"
    "&p=%3A30000"
    "&st=DEALER"
    "&sr=4"
    "&pw=74"
    "&dam=0"
    "&emc=EURO6"
    "&ref=dsp"
)

blocks = [
    (2013, 2015),
    (2016, 2018),
    (2019, 2021),
    (2022, 2026),
]

urls = [base + f"&fr={fr}&to={to}" for fr, to in blocks]

out = Path("src/scraping/search_urls.txt")
out.write_text("\n".join(urls), encoding="utf-8")

print("OK, creado", out, "con", len(urls), "búsquedas:")
print("\n".join(urls))
