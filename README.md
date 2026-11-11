# TAPpalti

Monitoraggio in streaming degli appalti pubblici italiani con i dati aperti ANAC.
Progetto per l'esame di Technologies for Advanced Programming (UniCT, prof. Salvatore Nicotra).

Ogni gara pubblicata da un ente (comuni, regioni, ASL...) attraversa una pipeline end-to-end:
il testo dell'oggetto viene classificato da un modello Spark MLlib (lavori, servizi o forniture),
confrontato con la categoria dichiarata dall'ente, e la gara riceve un punteggio di rischio
basato su indicatori noti ("red flags"). Tutto finisce in una dashboard Kibana che si aggiorna mentre i dati arrivano.

```
Dati ANAC (OCDS) -> replay.py -> Fluent Bit -> Kafka -> Spark Structured Streaming -> Elasticsearch -> Kibana
                                                              ^
                                               spark-trainer (modello MLlib addestrato sul 2024)
```

| Fase | Tecnologia | Cartella |
| --- | --- | --- |
| Sorgente | Python, replay cronologico dei dati ANAC | `ingestion/` |
| Ingestion | Fluent Bit 3.1 (`tail` -> `kafka`) | `fluent-bit/` |
| Streaming | Apache Kafka 4.1 (KRaft), topic `appalti` con 3 partizioni, Kafka UI | `docker-compose.yml` |
| Processing | Spark 3.4.3 Structured Streaming + MLlib | `spark/` |
| Indexing | Elasticsearch 8.13, indici `appalti` e `appalti-trend` con mapping esplicito | `setup/` |
| Visualization | Kibana 8.13 | `docs/dashboard-kibana.md` |

## Dati

Dati ANAC sui contratti pubblici in formato OCDS (Open Contracting Data Standard), pubblicati
sull'Open Contracting Data Registry: <https://data.open-contracting.org/en/publication/117>
(licenza CC BY 4.0, aggiornamento mensile, contratti sopra 40.000 euro).

Scarica dal browser e metti in `data/raw/`, senza estrarli:

- `2025.jsonl.gz` -> lo stream (cosa viene "riprodotto")
- `2024.jsonl.gz` -> l'addestramento del modello

In alternativa: `python ingestion/download.py 2024 2025`.

Perche' un replay: ANAC pubblica i dati in blocco una volta al mese, non come stream. `replay.py`
riordina le gare per data di pubblicazione e le emette a velocita' costante (`EVENTI_AL_SECONDO`),
quindi dal punto di vista di Kafka e Spark e' uno stream vero. La demo e' ripetibile e funziona anche senza internet.

## Requisiti

- Docker Desktop con almeno 6 GB di RAM assegnati (Settings -> Resources)
- La prima volta serve internet: Spark scarica i connettori Kafka ed Elasticsearch da Maven (poi restano in cache)

## Avvio

```bash
# 1. Addestra il modello (una volta, 5-10 minuti)
docker compose --profile training run --rm spark-trainer

# 2. Avvia la pipeline
docker compose up -d --build

# 3. Guarda cosa succede
docker compose logs -f replay spark-processor
```

| Interfaccia | Indirizzo |
| --- | --- |
| Kibana | <http://localhost:5601> |
| Kafka UI | <http://localhost:8080> |
| Spark UI | <http://localhost:4040> |
| Elasticsearch | <http://localhost:9200/appalti/_count> |

Per la dashboard segui `docs/dashboard-kibana.md`.

Stop senza perdere nulla: `docker compose stop`. Reset completo (topic, checkpoint, indici): `docker compose down -v`
(il modello in `models/` resta).

## Configurazione utile (in `docker-compose.yml`)

| Variabile | Servizio | Effetto |
| --- | --- | --- |
| `FILTRO_REGIONE` | replay | es. `SICILIA`: solo le gare degli enti siciliani |
| `FILTRO_ENTE` | replay | es. `ACI CASTELLO`: solo un ente |
| `EVENTI_AL_SECONDO` | replay | velocita' del replay (default 20) |
| `DATA_DA`, `MAX_GARE` | replay | finestra della demo |
| `ES_SINK` | spark-processor | `connector` (default) o `bulk` (piano B, vedi sotto) |

Dopo aver cambiato un filtro: `docker compose down -v` e poi `docker compose up -d`.

## Cosa calcola Spark

Per ogni gara (indice `appalti`, `_id` = id della gara):

- `settore`: join stream-statico con la tabella delle divisioni CPV (`spark/cpv_divisioni.csv`)
- `categoria_predetta`, `confidenza`: modello MLlib (RegexTokenizer -> StopWordsRemover italiano -> HashingTF -> IDF -> LogisticRegression multinomiale)
- `categoria_incoerente`: il modello e' sicuro almeno all'80% e la categoria dichiarata e' diversa
- indicatori di rischio e `rischio_punteggio` (0-100):

| Indicatore | Regola | Peso |
| --- | --- | --- |
| `rf_procedura_non_competitiva` | procedura OCDS `limited` o `direct` | 30 |
| `rf_sotto_soglia` | importo tra il 90% e il 100% della soglia di affidamento diretto (art. 50 D.Lgs. 36/2023: 150.000 euro lavori, 140.000 servizi e forniture) | 25 |
| `rf_offerente_unico` | un solo offerente | 25 |
| `rf_tempi_brevi` | meno di 10 giorni per presentare le offerte | 20 |

`rischio_livello`: alto da 60, medio da 30. Sono indicatori statistici di attenzione, non prove di irregolarita'.

### Limiti dei dati ANAC reali

Nei file ANAC alcuni campi OCDS sono vuoti; `ingestion/ocds.py` li gestisce cosi':

- **Regione**: assente. Ricavata dalle prime due cifre del CAP dell'ente (o dalla citta' se il CAP e' "N.A."): trovata per circa il 98% delle gare.
- **Procedura** (`procurementMethod`): vuota. Ricavata dal testo di `procurementMethodDetails` (affidamento diretto -> `direct`, negoziata -> `limited`, aperta -> `open`, ristretta -> `selective`).
- **Numero di offerenti** (`numberOfTenderers`): non pubblicato. L'indicatore `rf_offerente_unico` resta nel codice ma oggi non scatta mai.
- **Periodo offerte**: negli affidamenti diretti inizio = fine; 0 giorni e' trattato come dato mancante, altrimenti quasi tutte le gare risulterebbero "tempi brevi".

Modello (`models/metriche.json`): 95.051 gare 2024 di training, 23.637 di test, accuracy 0,899, F1 0,899, baseline 0,408.

Per regione e giorno (indice `appalti-trend`): finestre di 1 giorno sulla data di pubblicazione (event time)
con watermark di 3 giorni: numero di gare, importo totale, rischio medio, gare a rischio alto.

## Tolleranza ai guasti

- `replay.py` salva quante gare ha inviato: se riparte, continua da li'.
- Fluent Bit salva la posizione nel file (`DB`): niente righe rimandate.
- Kafka conserva i messaggi (retention 7 giorni): se Spark e' fermo, i dati aspettano.
- Spark salva gli offset nel checkpoint e riprende dall'ultimo micro-batch confermato.
- In Elasticsearch l'`_id` e' l'id della gara: un batch riscritto sovrascrive, non duplica.

## Problemi noti e soluzioni

| Sintomo | Soluzione |
| --- | --- |
| `replay` scrive "Nessun file trovato" | Metti `2025.jsonl.gz` in `data/raw/` |
| `spark-processor` dice "modello non trovato" | Esegui il training (passo 1). Lo stream funziona comunque, senza la parte ML |
| Errori del connettore `elasticsearch-spark` nei log di Spark | In `docker-compose.yml` metti `ES_SINK: bulk` e `docker compose up -d spark-processor` |
| Kibana non mostra dati | Controlla l'intervallo di tempo in alto a destra (es. "Last 1 hour") e che `http://localhost:9200/appalti/_count` sia maggiore di 0 |
| Elasticsearch si chiude subito | Aumenta la RAM di Docker Desktop |

## Test

La parte Python ha test automatici (normalizzazione OCDS, replay, preparazione del training):

```bash
python -m unittest discover -s tests -v
```

Per vedere com'e' fatto un file ANAC e quanto sono pieni i campi:

```bash
docker compose run --rm replay python inspect_ocds.py /data/raw/2025.jsonl.gz 2000
```

## Struttura

```
docker-compose.yml        tutti i servizi
ingestion/                sorgente: normalizzazione OCDS (ocds.py), replay, training set, download
fluent-bit/               configurazione di Fluent Bit
spark/                    processor.py (streaming), train_model.py (MLlib), tabella CPV, Dockerfile
setup/                    index template di Elasticsearch e data view di Kibana
docs/                     guida alla dashboard Kibana
tests/                    test della parte Python
data/raw/                 i file ANAC (non nel repository)
models/                   modello addestrato e metriche (generati)
```

