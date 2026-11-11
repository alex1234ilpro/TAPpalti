# Dashboard Kibana di TAPpalti (circa 10 minuti)

Le data view `Appalti (gare)` e `Appalti (trend giornaliero)` le crea il servizio `setup-kibana`.
La dashboard la costruisci tu con Lens: all'orale e' un vantaggio sapere come e' fatta ogni visualizzazione.

1. Apri <http://localhost:5601> -> menu -> **Dashboards** -> **Create dashboard** -> **Create visualization**.
2. In alto a destra imposta l'intervallo **Last 1 hour** e l'aggiornamento automatico ogni 10 secondi
   (icona calendario -> Refresh every 10 seconds). In alto a sinistra scegli la data view **Appalti (gare)**.

Crea queste visualizzazioni (dopo ognuna: **Save and return**).

| # | Titolo | Tipo Lens | Configurazione |
| --- | --- | --- | --- |
| 1 | Gare elaborate | Metric | Count of records |
| 2 | Importo totale | Metric | Sum of `importo` |
| 3 | Gare per livello di rischio | Donut | Slice by `rischio_livello`, metrica Count |
| 4 | Spesa per settore | Bar horizontal | Vertical axis: Top 10 values of `settore`; Horizontal: Sum of `importo` |
| 5 | Gare per regione | Bar vertical | Horizontal axis: Top 20 values of `regione`; Vertical: Count; Breakdown: `rischio_livello` |
| 6 | Gare nel tempo | Line | Horizontal: `@timestamp`; Vertical: Count |
| 7 | Categoria dichiarata vs stimata | Table | Rows: `categoria_dichiarata`; Columns: `categoria_predetta`; Metric: Count |
| 8 | Gare a rischio alto | Table | Rows: `ente.keyword`, `oggetto.keyword`; Metric: Max of `rischio_punteggio`, Sum of `importo`; filtro KQL `rischio_livello : "alto"` |

Per il trend: crea un'altra visualizzazione con data view **Appalti (trend giornaliero)**, tipo Line,
asse orizzontale `finestra_inizio`, verticale Sum of `importo_totale`, breakdown Top 5 of `regione`.
Per vederla usa un intervallo di tempo ampio (es. Last 2 years), perche' il tempo qui e' la data della gara.

3. **Save** la dashboard con il nome `TAPpalti`.
4. Facoltativo: menu -> Stack Management -> Saved Objects -> seleziona la dashboard -> **Export**, e salva
   il file in `docs/` per poterla reimportare dopo un `docker compose down -v`.

Filtri utili durante la demo (barra di ricerca KQL):

- `regione : "SICILIA"`
- `ente : "aci castello"` (ricerca sul nome dell’ente, maiuscole indifferenti)
- `categoria_incoerente : true` (gare dove la categoria dichiarata non torna con l'oggetto)
- `oggetto : manutenzione strade` (ricerca full-text con analizzatore italiano)
