"""
Scarica i file annuali ANAC (OCDS) dall'Open Contracting Data Registry in data/raw.
Se il download automatico non funziona, scaricali dal browser:
https://data.open-contracting.org/en/publication/117

Uso:  python download.py 2024 2025
"""

from __future__ import annotations

import os
import sys
import urllib.request

URL = "https://data.open-contracting.org/en/publication/117/download?name={anno}.jsonl.gz"
CARTELLA = os.getenv("RAW_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "raw"))


def scarica(anno: str) -> None:
    os.makedirs(CARTELLA, exist_ok=True)
    dest = os.path.join(CARTELLA, f"{anno}.jsonl.gz")
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print(f"{dest} esiste gia'")
        return
    req = urllib.request.Request(URL.format(anno=anno), headers={"User-Agent": "TAPpalti-university-project"})
    print(f"Scarico {anno} ...")
    with urllib.request.urlopen(req, timeout=120) as r, open(dest + ".part", "wb") as f:
        while blocco := r.read(1 << 20):
            f.write(blocco)
    os.replace(dest + ".part", dest)
    print(f"Salvato {dest} ({os.path.getsize(dest) / 1e6:.1f} MB)")


if __name__ == "__main__":
    for a in sys.argv[1:] or ["2024", "2025"]:
        scarica(a)
