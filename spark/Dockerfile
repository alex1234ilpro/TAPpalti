# Immagine Spark del progetto: Spark 3.4.3 (Scala 2.12, Python 3.8) + numpy (1.24.4 è l’ultima versione per Python 3.8), richiesto da pyspark.ml.
FROM apache/spark:3.4.3

USER root
RUN pip install --no-cache-dir numpy==1.24.4

# Il codice viene montato come volume in /opt/tappalti (vedi docker-compose.yml)
WORKDIR /opt/tappalti
