"""
Normalizzazione dei dati ANAC in formato OCDS (Open Contracting Data Standard).

Il file ANAC pubblicato sull'OCP Data Registry ha una riga JSON per ogni procedura
d'appalto. Il formato OCDS e' annidato e quasi tutti i campi sono opzionali, quindi qui
trasformiamo ogni procedura in un record "piatto" con nomi di campo fissi.

Questo modulo e' l'UNICO punto in cui si decidono i nomi dei campi: replay.py,
prepare_training.py e lo schema Spark usano tutti CAMPI (vedi sotto). Cosi' non puo'
succedere l'errore dell'esempio dei rifugi (station_name contro hut_name).

Solo libreria standard: nessuna dipendenza da installare.
"""

from __future__ import annotations

import gzip
import io
import json
import re
from datetime import date
from typing import Any, Iterator

# Nomi e tipi dei campi del record normalizzato (stesso ordine dello schema Spark).
CAMPI: dict[str, str] = {
    "id_gara": "string",            # ocid OCDS, identificativo unico della procedura
    "data_pubblicazione": "string",  # AAAA-MM-GG
    "ente": "string",                # stazione appaltante (buyer)
    "ente_id": "string",             # identificativo dell'ente (di solito codice fiscale)
    "comune": "string",              # localita' dell'ente, se presente
    "regione": "string",             # regione dell'ente, se presente
    "oggetto": "string",             # oggetto della gara (testo per il modello)
    "categoria_dichiarata": "string",  # lavori | servizi | forniture
    "cpv": "string",                 # codice CPV principale, 8 cifre
    "cpv_divisione": "string",       # prime 2 cifre del CPV
    "importo": "double",             # euro
    "procedura": "string",           # open | selective | limited | direct
    "procedura_dettaglio": "string",  # descrizione ANAC della procedura
    "num_offerenti": "integer",
    "giorni_presentazione": "integer",  # durata del periodo per presentare offerte
    "aggiudicatario": "string",
}

# mainProcurementCategory OCDS -> categoria in italiano
CATEGORIE_OCDS = {"works": "lavori", "services": "servizi", "goods": "forniture"}


def categoria_da_cpv(divisione: str | None) -> str | None:
    """Categoria dal CPV: 45 = lavori, 03-44 e 48 = forniture, 50-98 = servizi."""
    if not divisione or not divisione.isdigit():
        return None
    d = int(divisione)
    if d == 45:
        return "lavori"
    if 3 <= d <= 44 or d == 48:
        return "forniture"
    if 50 <= d <= 98:
        return "servizi"
    return None


# ---------------------------------------------------------------------------
# Lettura dei file
# ---------------------------------------------------------------------------

def apri_testo(percorso: str) -> io.TextIOBase:
    """Apre .jsonl o .jsonl.gz in testo UTF-8."""
    if percorso.endswith(".gz"):
        return gzip.open(percorso, "rt", encoding="utf-8")
    return open(percorso, "r", encoding="utf-8")


def release_da_riga(oggetto: Any) -> Iterator[dict]:
    """
    Una riga puo' essere: una release (o compiled release), un record
    {"ocid", "compiledRelease", "releases"}, oppure un package {"releases": [...]}
    / {"records": [...]}. Restituisce sempre le release da normalizzare.
    """
    if not isinstance(oggetto, dict):
        return
    if "compiledRelease" in oggetto and isinstance(oggetto["compiledRelease"], dict):
        yield oggetto["compiledRelease"]
        return
    if "records" in oggetto and isinstance(oggetto["records"], list):
        for rec in oggetto["records"]:
            yield from release_da_riga(rec)
        return
    if "ocid" in oggetto:
        yield oggetto
        return
    if "releases" in oggetto and isinstance(oggetto["releases"], list):
        for rel in oggetto["releases"]:
            if isinstance(rel, dict) and "ocid" in rel:
                yield rel


def leggi_release(percorso: str) -> Iterator[dict]:
    """Legge un file OCDS riga per riga e restituisce le release. Salta le righe rotte."""
    with apri_testo(percorso) as f:
        for riga in f:
            riga = riga.strip()
            if not riga:
                continue
            try:
                yield from release_da_riga(json.loads(riga))
            except json.JSONDecodeError:
                continue


# ---------------------------------------------------------------------------
# Funzioni di supporto
# ---------------------------------------------------------------------------

def _d(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def _l(x: Any) -> list:
    return x if isinstance(x, list) else []


def _testo(x: Any) -> str | None:
    if x is None:
        return None
    s = re.sub(r"\s+", " ", str(x)).strip()
    return s or None


def _numero(x: Any) -> float | None:
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return float(x) if x >= 0 else None
    if isinstance(x, str):
        s = x.strip().replace(" ", "")
        # accetta "1234.5" e "1.234,50"
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            v = float(s)
            return v if v >= 0 else None
        except ValueError:
            return None
    return None


_RE_DATA = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def _data(x: Any) -> date | None:
    """Prende AAAA-MM-GG all'inizio di una stringa ISO; scarta le date impossibili."""
    if not isinstance(x, str):
        return None
    m = _RE_DATA.match(x.strip())
    if not m:
        return None
    try:
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None
    return d if 2000 <= d.year <= 2035 else None


def _importo(v: Any) -> float | None:
    return _numero(_d(v).get("amount"))


def _cpv(rel: dict, tender: dict) -> str | None:
    """Primo codice CPV trovato in tender.classification, items, lots o awards."""
    candidati: list[dict] = []
    candidati.append(_d(tender.get("classification")))
    for item in _l(tender.get("items")):
        candidati.append(_d(_d(item).get("classification")))
    for award in _l(rel.get("awards")):
        for item in _l(_d(award).get("items")):
            candidati.append(_d(_d(item).get("classification")))
    for c in candidati:
        schema = str(c.get("scheme", "CPV")).upper()
        codice = re.sub(r"\D", "", str(c.get("id", "")))
        if "CPV" in schema and len(codice) >= 8:
            return codice[:8]
    return None


def _parte_ente(rel: dict) -> dict:
    """La party con ruolo buyer/procuringEntity, o quella con l'id di rel.buyer."""
    buyer_id = _d(rel.get("buyer")).get("id")
    parti = [_d(p) for p in _l(rel.get("parties"))]
    for p in parti:
        if buyer_id is not None and p.get("id") == buyer_id:
            return p
    for ruolo in ("buyer", "procuringEntity"):
        for p in parti:
            if ruolo in _l(p.get("roles")):
                return p
    return {}


# ---------------------------------------------------------------------------
# Normalizzazione
# ---------------------------------------------------------------------------

# I dati ANAC non hanno la regione dell'ente: la ricavo dalle prime due cifre del CAP (provincia).
_CAP_REGIONE = {}
for _r, _pref in {
    "LAZIO": "00 01 02 03 04", "UMBRIA": "05 06", "SARDEGNA": "07 08 09",
    "PIEMONTE": "10 12 13 14 15 28", "VALLE D'AOSTA": "11", "LIGURIA": "16 17 18 19",
    "LOMBARDIA": "20 21 22 23 24 25 26 27 46", "EMILIA-ROMAGNA": "29 40 41 42 43 44 47 48",
    "VENETO": "30 31 32 35 36 37 45", "FRIULI-VENEZIA GIULIA": "33 34", "TRENTINO-ALTO ADIGE": "38 39",
    "TOSCANA": "50 51 52 53 54 55 56 57 58 59", "MARCHE": "60 61 62 63", "ABRUZZO": "64 65 66 67",
    "PUGLIA": "70 71 72 73 74 76", "BASILICATA": "75 85", "CAMPANIA": "80 81 82 83 84",
    "MOLISE": "86", "CALABRIA": "87 88 89", "SICILIA": "90 91 92 93 94 95 96 97 98",
}.items():
    for _p in _pref.split():
        _CAP_REGIONE[_p] = _r


# Quando il CAP manca ("N.A.") provo con le citta' piu' frequenti
_CITTA_REGIONE = {
    "ROMA": "LAZIO", "MILANO": "LOMBARDIA", "NAPOLI": "CAMPANIA", "TORINO": "PIEMONTE", "PALERMO": "SICILIA",
    "GENOVA": "LIGURIA", "BOLOGNA": "EMILIA-ROMAGNA", "FIRENZE": "TOSCANA", "BARI": "PUGLIA", "CATANIA": "SICILIA",
    "VENEZIA": "VENETO", "VERONA": "VENETO", "MESSINA": "SICILIA", "PADOVA": "VENETO", "TRIESTE": "FRIULI-VENEZIA GIULIA",
    "BRESCIA": "LOMBARDIA", "BERGAMO": "LOMBARDIA", "TRENTO": "TRENTINO-ALTO ADIGE", "BOLZANO": "TRENTINO-ALTO ADIGE",
    "CAGLIARI": "SARDEGNA", "PERUGIA": "UMBRIA", "ANCONA": "MARCHE", "L'AQUILA": "ABRUZZO", "PESCARA": "ABRUZZO",
    "POTENZA": "BASILICATA", "CAMPOBASSO": "MOLISE", "CATANZARO": "CALABRIA", "REGGIO CALABRIA": "CALABRIA",
    "AOSTA": "VALLE D'AOSTA", "CUNEO": "PIEMONTE", "MODENA": "EMILIA-ROMAGNA", "PARMA": "EMILIA-ROMAGNA",
    "CARPI": "EMILIA-ROMAGNA", "SALERNO": "CAMPANIA", "TARANTO": "PUGLIA", "LECCE": "PUGLIA", "SIRACUSA": "SICILIA",
}


def regione_da_cap(cap, citta=None) -> "str | None":
    cap = re.sub(r"\D", "", str(cap or ""))
    if len(cap) == 5 and cap[:2] in _CAP_REGIONE:
        return _CAP_REGIONE[cap[:2]]
    return _CITTA_REGIONE.get(str(citta or "").strip().upper())


def procedura_da_dettaglio(dettaglio) -> "str | None":
    """ANAC non compila procurementMethod: lo ricavo dal testo, es. 'COD:24 ; TITLE:AFFIDAMENTO DIRETTO'."""
    t = str(dettaglio or "").upper()
    if not t:
        return None
    if "AFFIDAMENTO" in t or "COTTIMO" in t:
        return "direct"
    if "NEGOZIATA" in t:
        return "limited"
    if "APERTA" in t or "PROCEDURA DI GARA" in t:
        return "open"
    if any(k in t for k in ("RISTRETTA", "SELETTIVA", "DIALOGO", "COMPETITIVA", "PARTENARIATO", "PARTERNARIATO")):
        return "selective"
    return None


def normalizza(rel: dict) -> dict | None:
    """Trasforma una release OCDS in un record piatto con i campi di CAMPI.
    Restituisce None se manca l'identificativo della procedura."""
    ocid = _testo(rel.get("ocid"))
    if not ocid:
        return None

    tender = _d(rel.get("tender"))
    lotti = [_d(x) for x in _l(tender.get("lots"))]
    awards = [_d(x) for x in _l(rel.get("awards"))]
    contracts = [_d(x) for x in _l(rel.get("contracts"))]

    # Oggetto della gara: titolo, poi descrizione, poi lotti e aggiudicazioni
    oggetto = (
        _testo(tender.get("title"))
        or _testo(tender.get("description"))
        or next((_testo(l.get("title")) or _testo(l.get("description")) for l in lotti
                 if _testo(l.get("title")) or _testo(l.get("description"))), None)
        or next((_testo(a.get("title")) or _testo(a.get("description")) for a in awards
                 if _testo(a.get("title")) or _testo(a.get("description"))), None)
    )

    # Importo: valore della gara, poi somma dei lotti, poi aggiudicazione, poi contratto
    importo = _importo(tender.get("value"))
    if importo is None:
        valori_lotti = [v for v in (_importo(l.get("value")) for l in lotti) if v is not None]
        if valori_lotti:
            importo = sum(valori_lotti)
    if importo is None:
        importo = next((v for v in (_importo(a.get("value")) for a in awards) if v is not None), None)
    if importo is None:
        importo = next((v for v in (_importo(c.get("value")) for c in contracts) if v is not None), None)

    # Data: pubblicazione della gara, inizio periodo offerte, data della release, aggiudicazione
    periodo = _d(tender.get("tenderPeriod"))
    data = (
        _data(tender.get("datePublished"))
        or _data(periodo.get("startDate"))
        or _data(rel.get("date"))
        or next((d for d in (_data(a.get("date")) for a in awards) if d), None)
    )
    inizio, fine = _data(periodo.get("startDate")), _data(periodo.get("endDate"))
    giorni = (fine - inizio).days if inizio and fine and fine >= inizio else None
    if giorni == 0:
        giorni = None  # inizio = fine: nessun vero periodo per le offerte (tipico degli affidamenti diretti)

    # Ente
    buyer = _d(rel.get("buyer"))
    parte = _parte_ente(rel)
    indirizzo = _d(parte.get("address"))
    identificativo = _d(parte.get("identifier"))
    ente = _testo(buyer.get("name")) or _testo(parte.get("name"))
    ente_id = _testo(identificativo.get("id")) or _testo(buyer.get("id")) or _testo(parte.get("id"))

    # Categoria: quella dichiarata, altrimenti ricavata dal CPV
    cpv = _cpv(rel, tender)
    divisione = cpv[:2] if cpv else None
    categoria = CATEGORIE_OCDS.get(str(tender.get("mainProcurementCategory", "")).lower())
    if categoria is None:
        categoria = categoria_da_cpv(divisione)

    # Numero di offerenti
    num = tender.get("numberOfTenderers")
    num_offerenti = int(num) if isinstance(num, (int, float)) and not isinstance(num, bool) and num >= 0 else None

    # Primo aggiudicatario
    aggiudicatario = None
    for a in awards:
        for s in _l(a.get("suppliers")):
            aggiudicatario = _testo(_d(s).get("name"))
            if aggiudicatario:
                break
        if aggiudicatario:
            break

    regione = _testo(indirizzo.get("region")) or regione_da_cap(indirizzo.get("postalCode"), indirizzo.get("locality"))
    comune = _testo(indirizzo.get("locality"))

    return {
        "id_gara": ocid,
        "data_pubblicazione": data.isoformat() if data else None,
        "ente": ente,
        "ente_id": ente_id,
        "comune": comune.upper() if comune else None,
        "regione": regione.upper() if regione else None,
        "oggetto": oggetto,
        "categoria_dichiarata": categoria,
        "cpv": cpv,
        "cpv_divisione": divisione,
        "importo": importo,
        "procedura": _testo(tender.get("procurementMethod")) or procedura_da_dettaglio(tender.get("procurementMethodDetails")),
        "procedura_dettaglio": _testo(tender.get("procurementMethodDetails")),
        "num_offerenti": num_offerenti,
        "giorni_presentazione": giorni,
        "aggiudicatario": aggiudicatario,
    }


def normalizza_file(percorso: str) -> Iterator[dict]:
    for rel in leggi_release(percorso):
        rec = normalizza(rel)
        if rec is not None:
            yield rec


def passa_filtri(rec: dict, regione: str = "", ente: str = "") -> bool:
    """Filtri opzionali (sottostringa, maiuscole/minuscole indifferenti)."""
    if regione and regione.upper() not in (rec.get("regione") or ""):
        return False
    if ente and ente.upper() not in (rec.get("ente") or "").upper():
        return False
    return True
