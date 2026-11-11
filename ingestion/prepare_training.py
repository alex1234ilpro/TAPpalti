"""
Prepara il dataset di addestramento: normalizza le gare dell'anno di training (2024)
e tiene solo quelle con oggetto e categoria dichiarata. Le etichette sono REALI:
e' la categoria (lavori/servizi/forniture) che l'ente ha dichiarato ad ANAC.

Variabili d'ambiente:
  TRAIN_GLOB  file da usare            (default /data/raw/2024*.jsonl*)
  TRAIN_OUT   NDJSON di output         (default /state/training/appalti_train.ndjson)
  MAX_TRAIN   massimo numero di esempi (default 200000, campione casuale riproducibile)
"""

from __future__ import annotations

import glob
import json
import os
import random
import sys
from collections import Counter

from ocds import normalizza_file

TRAIN_GLOB = os.getenv("TRAIN_GLOB", "/data/raw/2024*.jsonl*")
TRAIN_OUT = os.getenv("TRAIN_OUT", "/state/training/appalti_train.ndjson")
MAX_TRAIN = int(os.getenv("MAX_TRAIN", "200000"))


def main() -> int:
    file = sorted(glob.glob(TRAIN_GLOB))
    if not file:
        print(f"[prepare] Nessun file per {TRAIN_GLOB}: scarica 2024.jsonl.gz in data/raw.", flush=True)
        return 1

    esempi: dict[str, dict] = {}
    for percorso in file:
        print(f"[prepare] Leggo {os.path.basename(percorso)} ...", flush=True)
        for rec in normalizza_file(percorso):
            if rec["oggetto"] and rec["categoria_dichiarata"]:
                esempi[rec["id_gara"]] = {"id_gara": rec["id_gara"], "oggetto": rec["oggetto"],
                                          "categoria_dichiarata": rec["categoria_dichiarata"]}

    righe = list(esempi.values())
    if MAX_TRAIN > 0 and len(righe) > MAX_TRAIN:
        random.Random(42).shuffle(righe)
        righe = righe[:MAX_TRAIN]

    if len(righe) < 100:
        print(f"[prepare] Solo {len(righe)} esempi utilizzabili: controlla il file.", flush=True)
        return 1

    os.makedirs(os.path.dirname(TRAIN_OUT), exist_ok=True)
    with open(TRAIN_OUT, "w", encoding="utf-8") as f:
        for r in righe:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[prepare] {len(righe)} esempi scritti in {TRAIN_OUT}", flush=True)
    for categoria, n in Counter(r["categoria_dichiarata"] for r in righe).most_common():
        print(f"[prepare]   {categoria:<10} {n:>8}  ({n / len(righe):.1%})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
