"""
Sorgente dello stream: riproduce le gare ANAC in ordine di data, come se arrivassero ora.

Legge i file OCDS in data/raw, normalizza ogni procedura (ocds.py), le ordina per data
di pubblicazione e scrive una riga JSON per gara nel file condiviso con Fluent Bit,
a una velocita' configurabile. Se il container riparte, riprende da dove era arrivato
(file di stato), cosi' Fluent Bit e Kafka non ricevono duplicati.

Variabili d'ambiente (tutte opzionali):
  RAW_GLOB          file da riprodurre             (default /data/raw/2025*.jsonl*)
  OUTPUT            file NDJSON letto da Fluent Bit (default /stream/appalti.ndjson)
  EVENTI_AL_SECONDO velocita' del replay            (default 20)
  FILTRO_REGIONE    es. SICILIA                     (default: tutte)
  FILTRO_ENTE       es. ACI CASTELLO                (default: tutti)
  DATA_DA           es. 2025-01-01                  (default: dall'inizio)
  MAX_GARE          numero massimo di gare, 0 = tutte (default 0)
"""

from __future__ import annotations

import glob
import json
import os
import sys
import time
from datetime import datetime, timezone

from ocds import normalizza_file, passa_filtri

RAW_GLOB = os.getenv("RAW_GLOB", "/data/raw/2025*.jsonl*")
OUTPUT = os.getenv("OUTPUT", "/stream/appalti.ndjson")
STATO = os.getenv("STATO", os.path.join(os.path.dirname(OUTPUT) or ".", "replay.state.json"))
EVENTI_AL_SECONDO = max(1, int(os.getenv("EVENTI_AL_SECONDO", "20")))
FILTRO_REGIONE = os.getenv("FILTRO_REGIONE", "").strip()
FILTRO_ENTE = os.getenv("FILTRO_ENTE", "").strip()
DATA_DA = os.getenv("DATA_DA", "").strip()
MAX_GARE = int(os.getenv("MAX_GARE", "0"))


def log(msg: str) -> None:
    print(f"[replay {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def aspetta_file() -> list[str]:
    """Aspetta che l'utente metta i file ANAC in data/raw invece di andare in crash."""
    while True:
        file = sorted(glob.glob(RAW_GLOB))
        if file:
            return file
        log(f"Nessun file trovato per {RAW_GLOB}. Scarica 2025.jsonl.gz da "
            "https://data.open-contracting.org/en/publication/117 e mettilo in data/raw. Riprovo tra 30 s.")
        time.sleep(30)


def firma(file: list[str]) -> str:
    """Identifica il replay: se cambiano file o filtri si riparte da zero."""
    parti = [f"{os.path.basename(f)}:{os.path.getsize(f)}" for f in file]
    parti += [FILTRO_REGIONE, FILTRO_ENTE, DATA_DA, str(MAX_GARE)]
    return "|".join(parti)


def leggi_stato() -> dict:
    try:
        with open(STATO, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def salva_stato(stato: dict) -> None:
    tmp = STATO + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(stato, f)
    os.replace(tmp, STATO)  # scrittura atomica


def carica_gare(file: list[str]) -> list[dict]:
    gare: dict[str, dict] = {}
    lette = senza_data = 0
    for percorso in file:
        log(f"Leggo {os.path.basename(percorso)} ...")
        for rec in normalizza_file(percorso):
            lette += 1
            if not passa_filtri(rec, FILTRO_REGIONE, FILTRO_ENTE):
                continue
            if not rec["data_pubblicazione"]:
                senza_data += 1
                continue
            if DATA_DA and rec["data_pubblicazione"] < DATA_DA:
                continue
            gare[rec["id_gara"]] = rec  # deduplica per id
    ordinate = sorted(gare.values(), key=lambda r: (r["data_pubblicazione"], r["id_gara"]))
    if MAX_GARE > 0:
        ordinate = ordinate[:MAX_GARE]
    log(f"Procedure lette: {lette}; scartate senza data: {senza_data}; da riprodurre: {len(ordinate)}")
    return ordinate


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    file = aspetta_file()
    gare = carica_gare(file)
    if not gare:
        log("Nessuna gara dopo i filtri: controlla FILTRO_REGIONE / FILTRO_ENTE / DATA_DA.")
        return

    stato = leggi_stato()
    sig = firma(file)
    inviate = stato.get("inviate", 0) if stato.get("firma") == sig else 0
    if inviate:
        log(f"Ripresa: {inviate} gare gia' inviate, continuo dalla {inviate + 1}.")
    elif stato:
        log("File o filtri cambiati: riparto da zero.")

    with open(OUTPUT, "a", encoding="utf-8") as out:
        while inviate < len(gare):
            inizio = time.monotonic()
            blocco = gare[inviate: inviate + EVENTI_AL_SECONDO]
            adesso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            for rec in blocco:
                out.write(json.dumps({**rec, "ingestion_time": adesso}, ensure_ascii=False) + "\n")
            out.flush()
            os.fsync(out.fileno())
            inviate += len(blocco)
            salva_stato({"firma": sig, "inviate": inviate})
            if inviate % (EVENTI_AL_SECONDO * 30) < EVENTI_AL_SECONDO:
                log(f"{inviate}/{len(gare)} gare inviate (data corrente {blocco[-1]['data_pubblicazione']})")
            time.sleep(max(0.0, 1.0 - (time.monotonic() - inizio)))

    log(f"Replay completato: {inviate} gare. Per rifarlo da capo: docker compose down -v")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
