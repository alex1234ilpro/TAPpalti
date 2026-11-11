"""
Strumento di controllo: mostra come sono fatti i dati OCDS di un file e quanto sono
pieni i campi normalizzati. Utile per l'orale ("come gestisci il formato dei dati?").

Uso:  python inspect_ocds.py /data/raw/2025.jsonl.gz [numero_righe]
"""

from __future__ import annotations

import json
import sys
from collections import Counter

from ocds import CAMPI, leggi_release, normalizza


def percorsi(obj, prefisso=""):
    """Tutti i percorsi di chiave (liste indicate con [])."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefisso}.{k}" if prefisso else k
            yield p
            yield from percorsi(v, p)
    elif isinstance(obj, list):
        for v in obj[:3]:
            yield from percorsi(v, prefisso + "[]")


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        return
    file, n = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    chiavi, pieni, esempi, tot = Counter(), Counter(), [], 0
    for rel in leggi_release(file):
        tot += 1
        chiavi.update(set(percorsi(rel)))
        rec = normalizza(rel)
        if rec:
            pieni.update(k for k, v in rec.items() if v not in (None, ""))
            if len(esempi) < 3:
                esempi.append(rec)
        if tot >= n:
            break

    print(f"Release analizzate: {tot}\n\nPercorsi OCDS piu' frequenti:")
    for k, c in chiavi.most_common(80):
        print(f"  {c / tot:6.1%}  {k}")
    print("\nCampi normalizzati pieni:")
    for k in CAMPI:
        print(f"  {pieni[k] / max(tot, 1):6.1%}  {k}")
    print("\nEsempi normalizzati:")
    for e in esempi:
        print(json.dumps(e, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
