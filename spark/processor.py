"""
TAPpalti - elaborazione in streaming con Spark Structured Streaming.

Kafka (topic "appalti")
  -> parsing JSON con schema esplicito
  -> join stream-statico con la tabella dei settori CPV
  -> indicatori di rischio ("red flags") e punteggio 0-100
  -> query 1: modello MLlib (categoria stimata dall'oggetto) -> Elasticsearch, indice "appalti"
  -> query 2: finestre giornaliere su event time con watermark -> Elasticsearch, indice "appalti-trend"

Due modi di scrivere su Elasticsearch (variabile ES_SINK):
  connector  connettore ufficiale elasticsearch-spark (default)
  bulk       foreachBatch + API _bulk via HTTP (piano B se il connettore da' problemi)
In entrambi i casi l'_id del documento e' l'id della gara: se un micro-batch viene
riscritto dopo un guasto, il documento viene sovrascritto e non duplicato.
"""

import json
import os
import urllib.request

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Confidenza = probabilita' massima del vettore "probability" del modello.
# UDF Python semplice: vector_to_array richiederebbe pandas, che l'immagine Spark non ha.
max_probabilita = F.udf(lambda v: float(max(v.toArray())) if v is not None else None, T.DoubleType())

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "appalti")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_SINK = os.getenv("ES_SINK", "connector")
MODEL_PATH = os.getenv("MODEL_PATH", "/opt/tappalti/models/categoria_appalti")
CPV_CSV = os.getenv("CPV_CSV", "/opt/tappalti/spark/cpv_divisioni.csv")
CHECKPOINTS = os.getenv("CHECKPOINTS", "/state/checkpoints")
WATERMARK = os.getenv("WATERMARK", "3 days")

ETICHETTE = ["lavori", "servizi", "forniture"]  # stesso ordine di train_model.py

# Soglie dell'affidamento diretto (art. 50 D.Lgs. 36/2023): lavori 150.000, servizi e forniture 140.000 euro
SOGLIA_LAVORI = 150000.0
SOGLIA_SERVIZI_FORNITURE = 140000.0

# Schema del JSON scritto da ingestion/replay.py (campi di ocds.CAMPI + ingestion_time)
SCHEMA = T.StructType([
    T.StructField("id_gara", T.StringType()),
    T.StructField("data_pubblicazione", T.StringType()),
    T.StructField("ente", T.StringType()),
    T.StructField("ente_id", T.StringType()),
    T.StructField("comune", T.StringType()),
    T.StructField("regione", T.StringType()),
    T.StructField("oggetto", T.StringType()),
    T.StructField("categoria_dichiarata", T.StringType()),
    T.StructField("cpv", T.StringType()),
    T.StructField("cpv_divisione", T.StringType()),
    T.StructField("importo", T.DoubleType()),
    T.StructField("procedura", T.StringType()),
    T.StructField("procedura_dettaglio", T.StringType()),
    T.StructField("num_offerenti", T.IntegerType()),
    T.StructField("giorni_presentazione", T.IntegerType()),
    T.StructField("aggiudicatario", T.StringType()),
    T.StructField("ingestion_time", T.StringType()),
])

ISO = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


def adesso_iso():
    return F.date_format(F.current_timestamp(), ISO)


# ---------------------------------------------------------------------------
# Trasformazioni
# ---------------------------------------------------------------------------

def leggi_kafka(spark: SparkSession) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")   # non perdere le gare gia' nel topic
            .option("maxOffsetsPerTrigger", 2000)     # micro-batch di dimensione limitata
            .option("failOnDataLoss", "false")
            .load())


def parse(raw: DataFrame) -> DataFrame:
    return (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("g"),
                       F.col("partition").alias("kafka_partition"),
                       F.col("offset").alias("kafka_offset"))
            .select("g.*", "kafka_partition", "kafka_offset")
            .filter(F.col("id_gara").isNotNull())
            .withColumn("regione", F.coalesce(F.col("regione"), F.lit("N/D")))
            .withColumn("procedura", F.lower(F.col("procedura"))))


def arricchisci_cpv(gare: DataFrame, cpv: DataFrame) -> DataFrame:
    """Join stream-statico: la tabella CPV e' piccola, la mandiamo a ogni executor (broadcast)."""
    return (gare.join(F.broadcast(cpv), on="cpv_divisione", how="left")
            .withColumn("settore", F.coalesce(F.col("settore"), F.lit("Non classificato"))))


def indicatori_rischio(df: DataFrame) -> DataFrame:
    cat = F.col("categoria_dichiarata")
    soglia = (F.when(cat == "lavori", F.lit(SOGLIA_LAVORI))
               .when(cat.isin("servizi", "forniture"), F.lit(SOGLIA_SERVIZI_FORNITURE)))
    df = (df.withColumn("soglia_affidamento_diretto", soglia)
          .withColumn("rf_procedura_non_competitiva",
                      F.coalesce(F.col("procedura").isin("limited", "direct"), F.lit(False)))
          .withColumn("rf_sotto_soglia",
                      F.coalesce((F.col("importo") >= F.col("soglia_affidamento_diretto") * 0.9)
                                 & (F.col("importo") < F.col("soglia_affidamento_diretto")), F.lit(False)))
          .withColumn("rf_offerente_unico", F.coalesce(F.col("num_offerenti") == 1, F.lit(False)))
          .withColumn("rf_tempi_brevi", F.coalesce(F.col("giorni_presentazione") < 10, F.lit(False))))
    punteggio = (F.col("rf_procedura_non_competitiva").cast("int") * 30
                 + F.col("rf_sotto_soglia").cast("int") * 25
                 + F.col("rf_offerente_unico").cast("int") * 25
                 + F.col("rf_tempi_brevi").cast("int") * 20)
    return (df.withColumn("rischio_punteggio", punteggio)
            .withColumn("rischio_livello", F.when(F.col("rischio_punteggio") >= 60, "alto")
                                            .when(F.col("rischio_punteggio") >= 30, "medio")
                                            .otherwise("basso")))


def applica_modello(df: DataFrame, modello) -> DataFrame:
    if modello is None:
        return (df.withColumn("categoria_predetta", F.lit(None).cast("string"))
                  .withColumn("confidenza", F.lit(None).cast("double"))
                  .withColumn("categoria_incoerente", F.lit(False)))
    etichette = F.array(*[F.lit(e) for e in ETICHETTE])
    out = modello.transform(df.withColumn("testo_modello", F.coalesce(F.col("oggetto"), F.lit(""))))
    return (out.withColumn("categoria_predetta", F.element_at(etichette, F.col("prediction").cast("int") + 1))
               .withColumn("confidenza", F.round(max_probabilita(F.col("probability")), 3))
               .withColumn("categoria_incoerente",
                           F.coalesce(F.col("categoria_dichiarata").isNotNull()
                                      & (F.col("categoria_predetta") != F.col("categoria_dichiarata"))
                                      & (F.col("confidenza") >= 0.8), F.lit(False))))


COLONNE_APPALTI = [
    "id_gara", "data_pubblicazione", "ente", "ente_id", "comune", "regione", "oggetto",
    "categoria_dichiarata", "categoria_predetta", "confidenza", "categoria_incoerente",
    "cpv", "cpv_divisione", "settore", "importo", "procedura", "procedura_dettaglio",
    "num_offerenti", "giorni_presentazione", "aggiudicatario", "soglia_affidamento_diretto",
    "rf_procedura_non_competitiva", "rf_sotto_soglia", "rf_offerente_unico", "rf_tempi_brevi",
    "rischio_punteggio", "rischio_livello", "ingestion_time", "kafka_partition", "kafka_offset",
]


def trend_giornaliero(df: DataFrame) -> DataFrame:
    """Aggregazione su finestre di 1 giorno di event time (data di pubblicazione) con watermark."""
    return (df.withColumn("ts_evento", F.to_timestamp(F.col("data_pubblicazione"), "yyyy-MM-dd"))
            .filter(F.col("ts_evento").isNotNull())
            .withWatermark("ts_evento", WATERMARK)
            .groupBy(F.window(F.col("ts_evento"), "1 day"), F.col("regione"))
            .agg(F.count(F.lit(1)).alias("n_gare"),
                 F.round(F.sum(F.coalesce(F.col("importo"), F.lit(0.0))), 2).alias("importo_totale"),
                 F.round(F.avg("rischio_punteggio"), 1).alias("rischio_medio"),
                 F.sum((F.col("rischio_livello") == "alto").cast("int")).alias("gare_rischio_alto"))
            .select(F.date_format(F.col("window.start"), "yyyy-MM-dd").alias("finestra_inizio"),
                    F.date_format(F.col("window.end"), "yyyy-MM-dd").alias("finestra_fine"),
                    "regione", "n_gare", "importo_totale", "rischio_medio", "gare_rischio_alto")
            .withColumn("id_trend", F.concat_ws("_", F.col("finestra_inizio"), F.col("regione")))
            .withColumn("@timestamp", adesso_iso()))


# ---------------------------------------------------------------------------
# Scrittura su Elasticsearch
# ---------------------------------------------------------------------------

def scrivi_connector(df: DataFrame, indice: str, id_col: str, nome: str, trigger: str):
    return (df.writeStream.format("org.elasticsearch.spark.sql")
            .queryName(nome).outputMode("append").trigger(processingTime=trigger)
            .option("checkpointLocation", f"{CHECKPOINTS}/{nome}")
            .option("es.nodes", ES_HOST).option("es.port", ES_PORT)
            .option("es.nodes.wan.only", "true")
            .option("es.mapping.id", id_col)
            .option("es.resource", indice)
            .start())


def scrivi_bulk(df: DataFrame, indice: str, id_col: str, nome: str, trigger: str):
    url = f"http://{ES_HOST}:{ES_PORT}/_bulk"

    def invia(batch: DataFrame, batch_id: int) -> None:
        righe = batch.toJSON().collect()   # micro-batch piccolo (maxOffsetsPerTrigger)
        if not righe:
            return
        corpo = []
        for r in righe:
            doc = json.loads(r)
            corpo.append(json.dumps({"index": {"_index": indice, "_id": doc[id_col]}}))
            corpo.append(r)
        req = urllib.request.Request(url, data=("\n".join(corpo) + "\n").encode("utf-8"),
                                     headers={"Content-Type": "application/x-ndjson"}, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            esito = json.loads(resp.read())
        if esito.get("errors"):
            primo = next(i for i in esito["items"] if "error" in i.get("index", {}))
            # Fallire qui = il batch non viene confermato nel checkpoint e verra' ripetuto
            raise RuntimeError(f"Errore Elasticsearch nel batch {batch_id}: {primo['index']['error']}")

    return (df.writeStream.queryName(nome).outputMode("append").trigger(processingTime=trigger)
            .option("checkpointLocation", f"{CHECKPOINTS}/{nome}")
            .foreachBatch(invia).start())


def main() -> None:
    spark = (SparkSession.builder.appName("TAPpalti-processor")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")   # pochi dati per batch: meno task
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    modello = None
    if os.path.isdir(MODEL_PATH):
        modello = PipelineModel.load(MODEL_PATH)
        print(f"[processor] modello caricato da {MODEL_PATH}", flush=True)
    else:
        print(f"[processor] ATTENZIONE: modello non trovato in {MODEL_PATH}. Lo stream parte senza ML; "
              "addestralo con: docker compose --profile training run --rm spark-trainer", flush=True)

    cpv = spark.read.csv(CPV_CSV, header=True, schema="cpv_divisione STRING, settore STRING")

    gare = indicatori_rischio(arricchisci_cpv(parse(leggi_kafka(spark)), cpv))

    appalti = (applica_modello(gare, modello)
               .select(*COLONNE_APPALTI, adesso_iso().alias("@timestamp")))
    trend = trend_giornaliero(gare)

    scrivi = scrivi_bulk if ES_SINK == "bulk" else scrivi_connector
    print(f"[processor] scrittura su Elasticsearch in modalita' '{ES_SINK}'", flush=True)
    query = [scrivi(appalti, "appalti", "id_gara", "appalti", "5 seconds"),
             scrivi(trend, "appalti-trend", "id_trend", "appalti_trend", "15 seconds")]

    # Log leggibile dei micro-batch (utile nella demo). awaitAnyTermination rilancia
    # l'eccezione se una query fallisce: il container esce e Docker lo riavvia, e Spark
    # riprende dal checkpoint.
    ultimi = {}
    while True:
        terminata = spark.streams.awaitAnyTermination(10)
        for q in query:
            p = q.lastProgress
            if p and ultimi.get(q.name) != p["batchId"]:
                ultimi[q.name] = p["batchId"]
                velocita = float(p.get("processedRowsPerSecond") or 0)
                print(f"[processor] {q.name}: batch {p['batchId']}, righe in ingresso {p['numInputRows']}, "
                      f"{velocita:.0f} righe/s", flush=True)
        if terminata:
            break


if __name__ == "__main__":
    main()
