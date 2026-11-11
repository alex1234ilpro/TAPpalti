"""
Configurazione iniziale, eseguita da un container che poi termina.

  python setup.py elasticsearch  -> crea gli index template (mapping esplicito) PRIMA che Spark scriva
  python setup.py kibana         -> crea le data view "appalti" e "appalti-trend" in Kibana

Idempotente: si puo' rilanciare senza creare doppioni. Solo libreria standard.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

ES = os.getenv("ES_URL", "http://elasticsearch:9200")
KIBANA = os.getenv("KIBANA_URL", "http://kibana:5601")

KW = {"type": "keyword"}
DATA = {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd"}

TEMPLATE_APPALTI = {
    "index_patterns": ["appalti"],
    "priority": 200,
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic": True,
            "properties": {
                "id_gara": KW, "ente_id": KW, "comune": KW, "regione": KW, "cpv": KW, "cpv_divisione": KW,
                "settore": KW, "categoria_dichiarata": KW, "categoria_predetta": KW, "procedura": KW,
                "procedura_dettaglio": KW, "aggiudicatario": KW, "rischio_livello": KW,
                "ente": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                # Testo per la ricerca full-text, con analizzatore italiano (stemming e stop word)
                "oggetto": {"type": "text", "analyzer": "italian",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "data_pubblicazione": DATA, "ingestion_time": DATA, "@timestamp": DATA,
                "importo": {"type": "double"}, "soglia_affidamento_diretto": {"type": "double"},
                "confidenza": {"type": "float"},
                "num_offerenti": {"type": "integer"}, "giorni_presentazione": {"type": "integer"},
                "rischio_punteggio": {"type": "integer"},
                "kafka_partition": {"type": "integer"}, "kafka_offset": {"type": "long"},
                "categoria_incoerente": {"type": "boolean"},
                "rf_procedura_non_competitiva": {"type": "boolean"}, "rf_sotto_soglia": {"type": "boolean"},
                "rf_offerente_unico": {"type": "boolean"}, "rf_tempi_brevi": {"type": "boolean"},
            },
        },
    },
}

TEMPLATE_TREND = {
    "index_patterns": ["appalti-trend"],
    "priority": 200,
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "id_trend": KW, "regione": KW,
                "finestra_inizio": DATA, "finestra_fine": DATA, "@timestamp": DATA,
                "n_gare": {"type": "long"}, "gare_rischio_alto": {"type": "long"},
                "importo_totale": {"type": "double"}, "rischio_medio": {"type": "float"},
            },
        },
    },
}

DATA_VIEW = [
    {"id": "appalti", "title": "appalti", "name": "Appalti (gare)", "timeFieldName": "@timestamp"},
    {"id": "appalti-trend", "title": "appalti-trend", "name": "Appalti (trend giornaliero)",
     "timeFieldName": "finestra_inizio"},
]


def richiesta(metodo: str, url: str, corpo: dict | None = None, headers: dict | None = None):
    dati = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    h = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=dati, headers=h, method=metodo)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        testo = e.read().decode("utf-8", "replace")
        return e.code, {"errore": testo}


def aspetta(url: str, nome: str, controllo) -> None:
    for tentativo in range(90):
        try:
            stato, corpo = richiesta("GET", url)
            if stato == 200 and controllo(corpo):
                print(f"[setup] {nome} pronto", flush=True)
                return
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            pass
        print(f"[setup] aspetto {nome} ({tentativo + 1}/90) ...", flush=True)
        time.sleep(5)
    raise SystemExit(f"[setup] {nome} non risponde")


def setup_elasticsearch() -> None:
    aspetta(f"{ES}/_cluster/health", "Elasticsearch", lambda c: c.get("status") in ("green", "yellow"))
    for nome, template in (("appalti", TEMPLATE_APPALTI), ("appalti-trend", TEMPLATE_TREND)):
        stato, corpo = richiesta("PUT", f"{ES}/_index_template/{nome}", template)
        if stato != 200:
            raise SystemExit(f"[setup] template {nome} non creato: {corpo}")
        print(f"[setup] index template '{nome}' ok", flush=True)


def setup_kibana() -> None:
    aspetta(f"{KIBANA}/api/status", "Kibana",
            lambda c: c.get("status", {}).get("overall", {}).get("level") == "available")
    for dv in DATA_VIEW:
        stato, corpo = richiesta("POST", f"{KIBANA}/api/data_views/data_view",
                                 {"data_view": dv, "override": True}, {"kbn-xsrf": "true"})
        if stato != 200:
            raise SystemExit(f"[setup] data view {dv['id']} non creata: {corpo}")
        print(f"[setup] data view '{dv['name']}' ok", flush=True)


if __name__ == "__main__":
    cosa = sys.argv[1] if len(sys.argv) > 1 else "elasticsearch"
    setup_kibana() if cosa == "kibana" else setup_elasticsearch()
