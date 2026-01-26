from pathlib import Path

p = Path(__file__).resolve().parent / "urls.txt"
print("PATH:", p)
print("EXISTS:", p.exists())
raw = p.read_bytes()
print("BYTES:", len(raw))
print("FIRST 200 BYTES:", raw[:200])

text = p.read_text(encoding="utf-8", errors="replace")
print("CHARS:", len(text))
print("LINES TOTAL:", len(text.splitlines()))
print("FIRST 5 LINES:")
for i, ln in enumerate(text.splitlines()[:5], start=1):
    print(i, repr(ln))