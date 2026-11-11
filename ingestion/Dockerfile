# Immagine della sorgente dati: solo libreria standard Python, nessuna dipendenza.
FROM python:3.11-slim

WORKDIR /app
COPY *.py ./

# -u: output non bufferizzato, i log compaiono subito in "docker compose logs"
CMD ["python", "-u", "replay.py"]
