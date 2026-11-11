"""
Test della parte Python (normalizzazione OCDS, replay, preparazione del training).
I dati qui sotto sono INVENTATI per il test e hanno la forma dello standard OCDS;
non sono gare reali.

Esecuzione (dalla cartella del progetto):  python -m unittest discover -s tests -v
"""

import gzip
import json
import os
import subprocess
import sys
import tempfile
import unittest

QUI = os.path.dirname(os.path.abspath(__file__))
INGESTION = os.path.join(QUI, "..", "ingestion")
sys.path.insert(0, INGESTION)

import ocds  # noqa: E402

RELEASE_COMPLETA = {
    "ocid": "ocds-test-0001",
    "date": "2025-02-03T10:00:00Z",
    "buyer": {"id": "IT-CF-00000000001", "name": "Comune di Esempio"},
    "parties": [{"id": "IT-CF-00000000001", "name": "Comune di Esempio", "roles": ["buyer"],
                 "identifier": {"scheme": "IT-CF", "id": "00000000001"},
                 "address": {"locality": "Esempio", "region": "Sicilia"}},
                {"id": "S1", "name": "Impresa Test srl", "roles": ["supplier"]}],
    "tender": {"title": "Lavori di manutenzione straordinaria della strada comunale",
               "mainProcurementCategory": "works", "procurementMethod": "direct",
               "procurementMethodDetails": "AFFIDAMENTO DIRETTO",
               "value": {"amount": 140000, "currency": "EUR"},
               "items": [{"classification": {"scheme": "CPV", "id": "45233141-9"}}],
               "numberOfTenderers": 1,
               "tenderPeriod": {"startDate": "2025-02-03", "endDate": "2025-02-08"}},
    "awards": [{"suppliers": [{"id": "S1", "name": "Impresa Test srl"}]}],
}

RELEASE_MINIMA = {  # niente categoria: va ricavata dal CPV; importo dai lotti con virgola decimale
    "ocid": "ocds-test-0002",
    "date": "2025-01-15",
    "tender": {"description": "Servizio di pulizia degli uffici",
               "lots": [{"value": {"amount": "1.000,50"}}, {"value": {"amount": 2000}}],
               "items": [{"classification": {"scheme": "CPV", "id": "90911200"}}]},
}


class TestNormalizzazione(unittest.TestCase):
    def test_campi_completi(self):
        r = ocds.normalizza(RELEASE_COMPLETA)
        self.assertEqual(set(r), set(ocds.CAMPI))
        self.assertEqual(r["id_gara"], "ocds-test-0001")
        self.assertEqual(r["categoria_dichiarata"], "lavori")
        self.assertEqual(r["cpv"], "45233141")
        self.assertEqual(r["cpv_divisione"], "45")
        self.assertEqual(r["importo"], 140000.0)
        self.assertEqual(r["regione"], "SICILIA")
        self.assertEqual(r["comune"], "ESEMPIO")
        self.assertEqual(r["ente_id"], "00000000001")
        self.assertEqual(r["procedura"], "direct")
        self.assertEqual(r["num_offerenti"], 1)
        self.assertEqual(r["giorni_presentazione"], 5)
        self.assertEqual(r["data_pubblicazione"], "2025-02-03")
        self.assertEqual(r["aggiudicatario"], "Impresa Test srl")

    def test_fallback(self):
        r = ocds.normalizza(RELEASE_MINIMA)
        self.assertEqual(r["oggetto"], "Servizio di pulizia degli uffici")
        self.assertEqual(r["categoria_dichiarata"], "servizi")   # dal CPV 90
        self.assertAlmostEqual(r["importo"], 3000.5)
        self.assertIsNone(r["regione"])

    def test_scarti(self):
        self.assertIsNone(ocds.normalizza({"tender": {"title": "senza ocid"}}))
        r = ocds.normalizza({"ocid": "x", "date": "2025-13-45", "tender": {"value": {"amount": -5}}})
        self.assertIsNone(r["data_pubblicazione"])
        self.assertIsNone(r["importo"])

    def test_categoria_da_cpv(self):
        self.assertEqual(ocds.categoria_da_cpv("45"), "lavori")
        self.assertEqual(ocds.categoria_da_cpv("30"), "forniture")
        self.assertEqual(ocds.categoria_da_cpv("48"), "forniture")
        self.assertEqual(ocds.categoria_da_cpv("72"), "servizi")
        self.assertIsNone(ocds.categoria_da_cpv(None))

    def test_forme_di_file(self):
        """Release semplice, record con compiledRelease, package, riga rotta: tutte gestite."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "t.jsonl.gz")
            with gzip.open(p, "wt", encoding="utf-8") as f:
                f.write(json.dumps(RELEASE_COMPLETA) + "\n")
                f.write(json.dumps({"ocid": "ocds-test-0002", "compiledRelease": RELEASE_MINIMA}) + "\n")
                f.write(json.dumps({"releases": [{**RELEASE_MINIMA, "ocid": "ocds-test-0003"}]}) + "\n")
                f.write("{riga rotta\n\n")
            ids = [r["id_gara"] for r in ocds.normalizza_file(p)]
            self.assertEqual(ids, ["ocds-test-0001", "ocds-test-0002", "ocds-test-0003"])

    def test_filtri(self):
        r = ocds.normalizza(RELEASE_COMPLETA)
        self.assertTrue(ocds.passa_filtri(r, "sicilia", ""))
        self.assertTrue(ocds.passa_filtri(r, "", "esempio"))
        self.assertFalse(ocds.passa_filtri(r, "LAZIO", ""))


def scrivi_raw(cartella, nome, release):
    p = os.path.join(cartella, nome)
    with gzip.open(p, "wt", encoding="utf-8") as f:
        for rel in release:
            f.write(json.dumps(rel) + "\n")
    return p


def righe_json(percorso):
    with open(percorso, encoding="utf-8") as f:
        return [json.loads(x) for x in f]


def esegui(script, env):
    return subprocess.run([sys.executable, os.path.join(INGESTION, script)],
                          env={**os.environ, **env}, capture_output=True, text=True, timeout=60)


class TestReplay(unittest.TestCase):
    def test_ordine_deduplica_e_ripresa(self):
        with tempfile.TemporaryDirectory() as d:
            release = [{**RELEASE_COMPLETA, "ocid": f"ocds-r-{i}", "date": f"2025-01-{28 - i:02d}",
                        "tender": {**RELEASE_COMPLETA["tender"], "tenderPeriod": {}}} for i in range(5)]
            release.append(release[0])  # duplicato
            scrivi_raw(d, "2025.jsonl.gz", release)
            out = os.path.join(d, "stream", "appalti.ndjson")
            env = {"RAW_GLOB": os.path.join(d, "2025*.jsonl*"), "OUTPUT": out, "EVENTI_AL_SECONDO": "100"}

            p = esegui("replay.py", env)
            self.assertEqual(p.returncode, 0, p.stderr)
            righe = righe_json(out)
            self.assertEqual(len(righe), 5)                       # duplicato rimosso
            date = [r["data_pubblicazione"] for r in righe]
            self.assertEqual(date, sorted(date))                  # ordine cronologico
            self.assertTrue(all("ingestion_time" in r for r in righe))

            # Seconda esecuzione: lo stato dice che e' tutto inviato, nessuna riga in piu'
            p = esegui("replay.py", env)
            self.assertEqual(p.returncode, 0, p.stderr)
            self.assertEqual(len(righe_json(out)), 5)

    def test_filtro_regione(self):
        with tempfile.TemporaryDirectory() as d:
            scrivi_raw(d, "2025.jsonl.gz", [RELEASE_COMPLETA, RELEASE_MINIMA])
            out = os.path.join(d, "appalti.ndjson")
            p = esegui("replay.py", {"RAW_GLOB": os.path.join(d, "2025*"), "OUTPUT": out,
                                     "FILTRO_REGIONE": "SICILIA", "EVENTI_AL_SECONDO": "100"})
            self.assertEqual(p.returncode, 0, p.stderr)
            self.assertEqual([r["id_gara"] for r in righe_json(out)], ["ocds-test-0001"])


class TestTraining(unittest.TestCase):
    def test_prepare(self):
        with tempfile.TemporaryDirectory() as d:
            release = [{**RELEASE_COMPLETA, "ocid": f"ocds-t-{i}"} for i in range(80)]
            release += [{**RELEASE_MINIMA, "ocid": f"ocds-s-{i}"} for i in range(40)]
            scrivi_raw(d, "2024.jsonl.gz", release)
            out = os.path.join(d, "training", "train.ndjson")
            p = esegui("prepare_training.py", {"TRAIN_GLOB": os.path.join(d, "2024*"), "TRAIN_OUT": out})
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            righe = righe_json(out)
            self.assertEqual(len(righe), 120)
            self.assertEqual(set(righe[0]), {"id_gara", "oggetto", "categoria_dichiarata"})


class TestCoerenzaSchema(unittest.TestCase):
    def test_schema_spark_uguale_a_campi(self):
        """Lo schema in spark/processor.py deve avere gli stessi campi di ocds.CAMPI (+ ingestion_time)."""
        with open(os.path.join(QUI, "..", "spark", "processor.py"), encoding="utf-8") as f:
            testo = f.read()
        inizio = testo.index("SCHEMA = T.StructType([")
        blocco = testo[inizio: testo.index("])", inizio)]
        import re
        campi_spark = re.findall(r'StructField\("([^"]+)"', blocco)
        self.assertEqual(campi_spark, list(ocds.CAMPI) + ["ingestion_time"])


if __name__ == "__main__":
    unittest.main()
