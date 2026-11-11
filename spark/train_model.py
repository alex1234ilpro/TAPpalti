"""
Addestramento del modello di TAPpalti (Spark MLlib), eseguito UNA volta prima dello stream.

Compito: dato l'oggetto di una gara, stimare la categoria (lavori / servizi / forniture).
Etichette reali: la categoria che l'ente ha dichiarato ad ANAC nelle gare del 2024.
Il modello poi gira sulle gare del 2025 che arrivano in streaming e segnala quelle in cui
la categoria dichiarata non torna con il testo dell'oggetto (controllo di qualita' del dato).

Pipeline: RegexTokenizer -> StopWordsRemover (italiano) -> HashingTF -> IDF -> LogisticRegression
"""

import json
import os
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IDF, HashingTF, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TRAIN_DATA = os.getenv("TRAIN_OUT", "/state/training/appalti_train.ndjson")
MODEL_PATH = os.getenv("MODEL_PATH", "/opt/tappalti/models/categoria_appalti")
METRICHE = os.getenv("METRICHE", "/opt/tappalti/models/metriche.json")

# Stesso ordine usato in processor.py per tradurre la predizione (0, 1, 2) in testo
ETICHETTE = ["lavori", "servizi", "forniture"]


def main() -> None:
    spark = (SparkSession.builder.appName("TAPpalti-training")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    dati = (spark.read.schema("id_gara STRING, oggetto STRING, categoria_dichiarata STRING")
            .json(TRAIN_DATA)
            .filter(F.col("categoria_dichiarata").isin(ETICHETTE))
            .withColumn("testo_modello", F.coalesce(F.col("oggetto"), F.lit("")))
            .withColumn("label", F.when(F.col("categoria_dichiarata") == "lavori", 0.0)
                                  .when(F.col("categoria_dichiarata") == "servizi", 1.0)
                                  .otherwise(2.0)))

    train, test = dati.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    n_train, n_test = train.count(), test.count()
    print(f"[training] esempi: train={n_train}, test={n_test}", flush=True)

    pipeline = Pipeline(stages=[
        # Parole = sequenze di lettere o cifre (Unicode: tiene le lettere accentate)
        RegexTokenizer(inputCol="testo_modello", outputCol="parole",
                       pattern="[^\\p{L}\\p{N}]+", minTokenLength=2, toLowercase=True),
        StopWordsRemover(inputCol="parole", outputCol="parole_filtrate",
                         stopWords=StopWordsRemover.loadDefaultStopWords("italian"),
                         locale="en_US"),
        HashingTF(inputCol="parole_filtrate", outputCol="tf", numFeatures=1 << 16),
        IDF(inputCol="tf", outputCol="features", minDocFreq=2),
        LogisticRegression(featuresCol="features", labelCol="label", family="multinomial",
                           maxIter=50, regParam=0.01),
    ])

    modello = pipeline.fit(train)
    predizioni = modello.transform(test).cache()

    valuta = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = valuta.evaluate(predizioni, {valuta.metricName: "accuracy"})
    f1 = valuta.evaluate(predizioni, {valuta.metricName: "f1"})

    # Termine di paragone: quanto farebbe chi risponde sempre con la classe piu' frequente
    piu_frequente = test.groupBy("label").count().orderBy(F.desc("count")).first()
    baseline = piu_frequente["count"] / n_test if n_test else 0.0

    print(f"[training] accuracy={accuracy:.3f}  f1={f1:.3f}  baseline(classe piu' frequente)={baseline:.3f}",
          flush=True)
    print("[training] matrice di confusione (righe = vera, colonne = predetta):", flush=True)
    matrice = predizioni.groupBy("label").pivot("prediction", [0.0, 1.0, 2.0]).count().orderBy("label")
    for r in matrice.collect():
        valori = [r[c] or 0 for c in matrice.columns[1:]]
        print(f"[training]   {ETICHETTE[int(r['label'])]:<10} {valori}", flush=True)

    modello.write().overwrite().save(MODEL_PATH)
    os.makedirs(os.path.dirname(METRICHE), exist_ok=True)
    with open(METRICHE, "w", encoding="utf-8") as f:
        json.dump({"addestrato_il": datetime.now(timezone.utc).isoformat(), "train": n_train, "test": n_test,
                   "accuracy": round(accuracy, 4), "f1": round(f1, 4), "baseline": round(baseline, 4)},
                  f, indent=2)
    print(f"[training] modello salvato in {MODEL_PATH}, metriche in {METRICHE}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
