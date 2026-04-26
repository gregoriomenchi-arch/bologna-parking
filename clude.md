# Bologna Parking App — Istruzioni per Claude Code

## Visione
App mobile per trovare parcheggio a Bologna in tempo reale, con
predizione oraria basata su dati reali. Obiettivo: pubblicazione
su App Store e Google Play entro 2 mesi (MVP serio, non demo).

## Vantaggio competitivo vs Google Maps / EasyPark
1. Predizione temporale ("tra 20 min via Saragozza si libera")
2. Integrazione ZTL Bologna (Sirio) con notifiche anti-multa
3. Contesto bolognese (UniBo, Bologna FC, Fiera, Unipol Arena)
4. Gratuito, no-account per l'MVP

## Stack
- Backend: Python 3.11 + FastAPI + httpx
- Database: PostgreSQL (Railway) — migrato da SQLite
- Frontend attuale: HTML + Leaflet.js (webapp, no build step)
- Deploy backend: Railway (production)
- Deploy frontend: GitHub Pages
- ML futuro: Prophet (serie storiche orarie)
- Versionamento: Git + GitHub

## URL produzione
- Webapp: https://gregoriomenchi-arch.github.io/bologna-parking
- Backend: https://bologna-parking-production.up.railway.app
- Repo: github.com/gregoriomenchi-arch/bologna-parking

## Struttura progetto
bologna-parking/
├── backend/
│   ├── main.py              # FastAPI app
│   ├── sostabo.py           # Client API SostaBo
│   ├── historical_data.py   # Score strade + dati storici
│   ├── eventi.py            # Scheletro eventi (scraper da completare)
│   ├── weather.py           # OpenWeatherMap — cache 10 min
│   ├── unibo.py             # Calendario UniBo AA 2025/2026
│   ├── ztl.py               # ZTL Sirio — orari + buffer effect
│   ├── traffic_collector.py # TomTom Flow API — correlazioni eventi
│   ├── osm_collector.py     # Overpass API — strisce blu + fallback statico
│   ├── db.py                # Adapter PostgreSQL/SQLite
│   ├── scheduler.py         # Script standalone locale (non Railway)
│   ├── venv/
│   └── requirements.txt
├── webapp/                  # HTML+JS (dev locale)
│   └── index.html
├── docs/                    # GitHub Pages (copia di webapp/)
│   └── index.html
├── claude.md                # Questo file
└── docs/ARCHITECTURE.md     # Visione originale

## Stato attuale (aggiornato al 26 aprile 2026)

### ✅ Completato
- Backend su Railway + PostgreSQL (production)
- Webapp su GitHub Pages (production)
- Integrazione SostaBo real-time (13 parcheggi scambiatori + 10 statici stimati)
- Heatmap 1185 strade di Bologna
- Modulo eventi (scheletro, no scraper reali)
- Scheduler integrato in FastAPI (asyncio background task, h24 su Railway)
- OSM collector (strisce blu fallback + strutture) con /osm/collect?force=true
- TomTom traffic collector con correlazioni eventi
- **Score strade ora basato su predittori reali** (completato 26/04):
  - `weather.py`: OpenWeatherMap (cache 10 min, chiave già in .env)
  - `unibo.py`: calendario AA 2025/2026, 3 zone impatto (Zamboni, Belmeloro, Sant'Orsola)
  - `ztl.py`: orari Sirio (lu-ve 7-20, sab 14-20, dom/festivi chiusa), buffer effect
  - Algoritmo: base storico ±20 pioggia ±15/25 UniBo ±30 evento ±10 ZTL buffer
  - Endpoint /condizioni/attive per webapp banner
  - Endpoint /debug/score/{via} per breakdown completo
  - Banner condizioni attive in webapp (pioggia, lezioni, ZTL)

### ⚠️ Problemi noti da risolvere
- Scraper eventi non implementati (solo scheletri + seed hardcoded)
- API Iperbole mai connessa (timeout)
- Parcheggi mancanti: Antistadio, Certosa, VIII Agosto, Nuovo Parcheggio Stazione
- Webapp non ottimizzata per mobile
- Nessuna privacy policy, nessuna icona app, nessun asset store
- OPENWEATHER_KEY già presente in backend/.env — da aggiungere anche in Railway env vars

## Roadmap 2 mesi (verso store)

### Settimana 1-2: Score reali
- [x] Integrazione OpenWeatherMap
- [x] Calendario UniBo (lezioni + esami)
- [x] ZTL Sirio (orari + notifiche)
- [ ] Completamento parcheggi scambiatori mancanti
- [x] Algoritmo score basato su fattori reali

### Settimana 3-4: Dati + validazione
- [ ] Scraper eventi reali (Bologna FC via football-data.org,
      Fiera via scraping bolognafiere.it)
- [ ] 10 interviste utente strutturate (bolognesi automobilisti)
- [ ] Analisi: quali feature vogliono davvero?
- [ ] Fix bug emersi dalle interviste

### Settimana 5-6: Mobile + UX
- [ ] Pivot webapp → app mobile con Capacitor
  (più veloce di React Native da zero; usa HTML/JS già fatto)
- [ ] Dark mode, icona, splash screen
- [ ] Push notifications ZTL
- [ ] Ricerca per destinazione ("voglio andare in via X")

### Settimana 7-8: Store
- [ ] Privacy policy + GDPR compliance
- [ ] Screenshot store + descrizione
- [ ] TestFlight (iOS) + Internal Testing (Android)
- [ ] Submission App Store + Google Play

## Convenzioni di sviluppo

### Git
- Commit atomici con messaggio chiaro (cosa + perché)
- Push su main dopo ogni sessione completata
- Tag `v0.x` a ogni milestone

### Codice
- Python: type hints ovunque, docstring sulle funzioni pubbliche
- Cache aggressiva per API esterne (rispetta quote gratuite)
- Log strutturato (loguru) per debug produzione
- Secrets SEMPRE in .env, MAI nel codice
- Ogni nuovo endpoint deve avere esempio in /docs (Swagger auto)

### Testing
- Endpoint /debug/* per ispezionare stato interno
- Per ogni nuovo fattore di score, debug breakdown disponibile
- Test manuale su webapp prima del commit

## Vincoli importanti

### Privacy / GDPR
- No raccolta dati utente senza consenso esplicito
- Se in futuro crowdsourcing GPS: DPIA obbligatoria prima
- Tutti i dati utente anonimizzati lato server

### API gratuite da rispettare
- OpenWeatherMap: 1000 chiamate/giorno → cache 10 min
- SostaBo: polite scraping, 1 chiamata ogni 10 min
- football-data.org: 10 chiamate/min → cache 1 ora
- Overpass/OSM: niente uso intensivo

### Performance
- Endpoint /parcheggi/disponibilita deve rispondere <500ms
- Mappa webapp deve caricare <2s su 4G

## Come iniziare ogni sessione con Claude Code

1. Apri terminale in bologna-parking/
2. `claude`
3. Primo messaggio: "Leggi claude.md, poi dimmi qual è il
   prossimo task prioritario secondo la roadmap"
4. Al termine sessione: aggiorna claude.md con cosa hai fatto

## Risorse esterne utili
- Open Data Bologna: dati.comune.bologna.it
- SostaBo: www.sostabo.it
- TPER real-time: solweb.tper.it
- OpenWeatherMap: openweathermap.org/api
- football-data.org: www.football-data.org
- Capacitor (per mobile): capacitorjs.com

## Contatto umano
Gregorio — obiettivo personale: pubblicare l'app sullo store
come progetto reale portfolio-worthy, non esercizio didattico.