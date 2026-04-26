"""
Bologna Parking API — Backend FastAPI
"""

import asyncio
import json
import logging
import logging.config
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from sostabo import SostaBoClient, ParcheggioDisponibilita, ParcheggioStruttura, ParcheggioZona, get_static_parcheggi
from historical_data import (
    init_db,
    save_readings,
    get_storico,
    compute_street_scores,
    compute_single_street_score,
)
from eventi import (
    init_events_db,
    seed_test_events,
    get_upcoming_events,
    get_active_and_soon,
)
from traffic_collector import (
    init_traffic_db,
    collect_traffic,
    get_storico_traffico,
    get_correlazioni_eventi,
    TOMTOM_KEY,
)
from osm_collector import (
    init_osm_db,
    collect_osm_data,
    get_osm_stats,
)
from weather import get_meteo, meteo_to_dict, OPENWEATHER_KEY
from unibo import is_giorno_lezioni, is_sessione_esami, get_status as unibo_status
from ztl import get_status as ztl_status, is_ztl_attiva

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bologna_parking")

STATIC_STREETS = Path(__file__).parent / "data" / "strade_bologna.json"

# ---------------------------------------------------------------------------
# Stato in-memory
# ---------------------------------------------------------------------------

_streets_geojson: dict | None = None   # GeoJSON base caricato da file statico
_streets_ready = False

# Stato scheduler — visibile in /health
_sched_cycle:        int = 0
_sched_total_saved:  int = 0
_sched_last_run:     datetime | None = None
_sched_last_count:   int = 0
_sched_last_error:   str | None = None

COLLECT_INTERVAL = 600  # 10 minuti


# ---------------------------------------------------------------------------
# Background task raccolta dati
# ---------------------------------------------------------------------------

async def _collect_loop() -> None:
    """Raccoglie dati SostaBo ogni 10 minuti e li salva nel DB."""
    global _sched_cycle, _sched_total_saved, _sched_last_run, _sched_last_count, _sched_last_error

    log.info("Scheduler avviato — intervallo %d s", COLLECT_INTERVAL)

    while True:
        _sched_cycle += 1
        ts = datetime.now(timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        try:
            # SostaBo — dati parcheggi
            async with SostaBoClient() as client:
                parcheggi = await client.get_disponibilita()
            save_readings(parcheggi)
            n = len(parcheggi)
            _sched_total_saved += n
            _sched_last_count  = n
            _sched_last_run    = ts
            _sched_last_error  = None
            log.info(
                "[scheduler ciclo %d] %s — parcheggi: %d record (totale: %d)",
                _sched_cycle, ts_str, n, _sched_total_saved,
            )

            # TomTom — dati traffico (non blocca se TOMTOM_KEY assente)
            eventi = get_active_and_soon(within_hours=2)
            n_traffic = await collect_traffic(eventi)
            if n_traffic:
                log.info(
                    "[scheduler ciclo %d] %s — traffico: %d punti salvati",
                    _sched_cycle, ts_str, n_traffic,
                )

        except Exception as exc:
            _sched_last_error = str(exc)
            log.warning("[scheduler ciclo %d] raccolta fallita: %s", _sched_cycle, exc)

        await asyncio.sleep(COLLECT_INTERVAL)



def _load_static_streets() -> None:
    """Carica il GeoJSON statico da data/strade_bologna.json (sincrono, istantaneo)."""
    global _streets_geojson, _streets_ready
    if STATIC_STREETS.exists():
        with open(STATIC_STREETS, encoding="utf-8") as f:
            _streets_geojson = json.load(f)
        log.info("Strade caricate da file statico (%d feature)", len(_streets_geojson["features"]))
    else:
        log.error("File %s non trovato — endpoint strade non disponibile", STATIC_STREETS)
        _streets_geojson = {"type": "FeatureCollection", "features": []}
    _streets_ready = True


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

async def _osm_startup_task() -> None:
    """Raccoglie dati OSM al primo avvio; no-op se i dati esistono già."""
    try:
        result = await collect_osm_data(force=False)
        if result["already_collected"]:
            log.info("OSM: dati già presenti — skip raccolta")
        else:
            log.info(
                "OSM: raccolta completata — %d strisce, %d strutture, %d ZTL, %d pedonali",
                result["parking_spots"], result["parking_lots"],
                result["ztl_zones"], result["pedestrian_areas"],
            )
    except Exception as exc:
        log.error("OSM startup task fallita: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_events_db()
    init_traffic_db()
    init_osm_db()
    seed_test_events()               # eventi hardcoded per testing immediato
    _load_static_streets()           # sincrono, <50 ms, nessuna chiamata esterna
    if TOMTOM_KEY:
        log.info("TomTom API key trovata — raccolta traffico abilitata")
    else:
        log.warning("TOMTOM_KEY non impostata — raccolta traffico disabilitata")
    if OPENWEATHER_KEY:
        log.info("OpenWeatherMap API key trovata — meteo abilitato")
    else:
        log.warning("OPENWEATHER_KEY non impostata — endpoint /meteo restituirà disponibile=false")
    asyncio.create_task(_collect_loop())
    asyncio.create_task(_osm_startup_task())   # una-tantum, idempotente
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Bologna Parking API",
    description="Dati in tempo reale sui parcheggi di Bologna via Open Data del Comune.",
    version="0.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Serve data/strade_bologna.json direttamente come file statico
app.mount("/data", StaticFiles(directory=str(STATIC_STREETS.parent)), name="data")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Sistema"])
async def health():
    async with SostaBoClient() as client:
        upstream_ok = await client.ping()
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "upstream_sostabo": "reachable" if upstream_ok else "unreachable",
        "streets_ready": _streets_ready,
        "streets_count": len(_streets_geojson["features"]) if _streets_geojson else 0,
        "scheduler": {
            "cycles": _sched_cycle,
            "total_records_saved": _sched_total_saved,
            "last_run": _sched_last_run.isoformat() if _sched_last_run else None,
            "last_count": _sched_last_count,
            "last_error": _sched_last_error,
            "interval_sec": COLLECT_INTERVAL,
        },
    }


@app.get(
    "/parcheggi/disponibilita",
    response_model=list[ParcheggioDisponibilita],
    tags=["Parcheggi"],
)
async def disponibilita(limit: int = Query(default=50, ge=1, le=200)):
    try:
        async with SostaBoClient() as client:
            return await client.get_disponibilita(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Errore upstream SostaBo: {exc}")


@app.get(
    "/parcheggi/zone",
    response_model=list[ParcheggioZona],
    tags=["Parcheggi"],
)
async def zone():
    try:
        async with SostaBoClient() as client:
            return await client.get_zone()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Errore Overpass API: {exc}")


@app.get(
    "/parcheggi/strutture",
    response_model=list[ParcheggioStruttura],
    tags=["Parcheggi"],
)
async def strutture(limit: int = Query(default=100, ge=1, le=500)):
    try:
        async with SostaBoClient() as client:
            return await client.get_strutture(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Errore upstream SostaBo: {exc}")


@app.get(
    "/parcheggi/statici",
    response_model=list[ParcheggioDisponibilita],
    tags=["Parcheggi"],
)
async def parcheggi_statici():
    """
    Restituisce i 10 grandi parcheggi/scambiatori con occupazione stimata per fascia oraria.
    Nessuna chiamata a SostaBo — dati sempre disponibili.
    """
    return get_static_parcheggi()


@app.get("/parcheggi/storico", tags=["Storico"])
async def storico():
    return get_storico()


@app.get("/eventi/prossimi", tags=["Eventi"])
async def eventi_prossimi(ore: int = Query(default=48, ge=1, le=168)):
    """
    Restituisce gli eventi nelle prossime `ore` ore (default 48).
    Ogni evento include nome, venue, data_inizio, coordinate, impatto e raggio_km.
    """
    return get_upcoming_events(hours=ore)


@app.get("/eventi/attivi", tags=["Eventi"])
async def eventi_attivi(entro_ore: int = Query(default=2, ge=1, le=24)):
    """Restituisce gli eventi attivi ora o che iniziano entro `entro_ore` ore."""
    return get_active_and_soon(within_hours=entro_ore)


@app.get("/osm/stats", tags=["OSM"])
async def osm_stats():
    """
    Conta i record OSM salvati nel DB per categoria.
    Mostra anche la data dell'ultima raccolta.
    """
    return get_osm_stats()


@app.get("/osm/collect", tags=["OSM"])
async def osm_collect(force: bool = False):
    """
    Avvia (o ri-avvia con force=true) la raccolta dati OSM da Overpass.
    force=true pulisce i dati esistenti e ri-raccoglie da zero.
    Normalmente non necessario: parte in automatico al primo deploy.
    """
    asyncio.create_task(collect_osm_data(force=force))
    return {"status": "avviato", "force": force}


@app.get("/traffico/storico", tags=["Traffico"])
async def traffico_storico():
    """
    Media velocità e congestione per ogni strada monitorata,
    per ora del giorno e giorno della settimana.
    Ritorna lista vuota se TOMTOM_KEY non è configurata.
    """
    return get_storico_traffico()


@app.get("/correlazioni/eventi", tags=["Traffico"])
async def correlazioni_eventi():
    """
    Impatto medio degli eventi sul traffico nelle zone circostanti,
    diviso per tipo (partita, fiera, concerto, teatro) e finestra temporale
    (3h prima, durante, 2h dopo).
    Ritorna lista vuota finché non ci sono dati storici sufficienti.
    """
    return get_correlazioni_eventi()


async def _safe_sostabo() -> list:
    try:
        async with SostaBoClient() as client:
            return await client.get_disponibilita()
    except Exception:
        return []


@app.get("/meteo/attuale", tags=["Meteo"])
async def meteo_attuale():
    """
    Condizioni meteo attuali per Bologna via OpenWeatherMap (cache 10 min).
    Ritorna disponibile=false se OPENWEATHER_KEY non è configurata.
    """
    meteo = await get_meteo()
    return meteo_to_dict(meteo)


@app.get("/ztl/status", tags=["ZTL"])
async def ztl_status_endpoint():
    """
    Stato attuale ZTL Sirio: attiva/non attiva, orario prossima attivazione/disattivazione.
    """
    return ztl_status()


@app.get("/unibo/status", tags=["UniBo"])
async def unibo_status_endpoint():
    """
    Stato calendario UniBo per oggi: giorno di lezione, sessione esami, pausa estiva.
    """
    return unibo_status()


@app.get("/condizioni/attive", tags=["Score"])
async def condizioni_attive():
    """
    Aggregato di tutti i fattori che influenzano lo score strade in questo momento.
    Usato dalla webapp per mostrare il banner informativo.
    """
    meteo  = await get_meteo()
    ztl_st = ztl_status()
    lezioni = is_giorno_lezioni()
    esami   = is_sessione_esami()

    fattori = []
    if meteo and meteo.pioggia:
        fattori.append({"codice": "pioggia", "label": f"Pioggia ({meteo.pioggia_mm:.1f} mm)", "emoji": "🌧️", "delta_score": -20})
    if esami:
        fattori.append({"codice": "esami", "label": "Sessione esami UniBo", "emoji": "📝", "delta_score": -25})
    elif lezioni:
        fattori.append({"codice": "lezioni", "label": "Giorno lezioni UniBo", "emoji": "🎓", "delta_score": -15})
    if ztl_st["attiva"]:
        ora_fine = ztl_st.get("prossima_disattivazione", "20:00")
        fattori.append({"codice": "ztl_attiva", "label": f"ZTL attiva fino {ora_fine}", "emoji": "🚫", "delta_score": 0})
    elif ztl_st["attiva_tra_30_min"]:
        ora_att = ztl_st.get("prossima_attivazione", "")
        fattori.append({"codice": "ztl_presto", "label": f"ZTL tra {ztl_st['minuti_a_attivazione']} min ({ora_att})", "emoji": "⚠️", "delta_score": -10})

    return {
        "fattori": fattori,
        "meteo": meteo_to_dict(meteo),
        "ztl": ztl_st,
        "unibo": {"giorno_lezioni": lezioni, "sessione_esami": esami},
    }


@app.get("/debug/score/{via}", tags=["Debug"])
async def debug_score(via: str):
    """
    Breakdown dettagliato dello score per una strada (ricerca per nome parziale).
    Es: /debug/score/Zamboni  →  score + tutti i fattori applicati.
    """
    if not _streets_ready or not _streets_geojson:
        raise HTTPException(status_code=503, detail="Strade non disponibili")

    meteo   = await get_meteo()
    ztl_st  = ztl_status()
    lezioni = is_giorno_lezioni()
    esami   = is_sessione_esami()
    eventi  = get_active_and_soon(within_hours=2)

    try:
        async with SostaBoClient() as client:
            live = await client.get_disponibilita()
    except Exception:
        live = []

    result = compute_single_street_score(
        via, _streets_geojson, live, eventi,
        pioggia=meteo.pioggia if meteo else False,
        unibo_lezioni=lezioni,
        unibo_esami=esami,
        ztl_attiva_tra_30_min=ztl_st["attiva_tra_30_min"],
    )
    if result is None:
        raise HTTPException(status_code=404, detail=f"Strada '{via}' non trovata nel GeoJSON")
    return result


@app.get("/strade/probabilita", tags=["Strade"])
async def probabilita_strade():
    """
    GeoJSON con score aggiornati in tempo reale.
    Integra: SostaBo live, storico DB, meteo, calendario UniBo, ZTL Sirio, eventi.
    """
    if not _streets_ready or not _streets_geojson or not _streets_geojson["features"]:
        raise HTTPException(status_code=503, detail="Strade non disponibili")

    # Fetch parallelo: SostaBo live + meteo (entrambi I/O bound)
    import asyncio as _asyncio
    live_task  = _asyncio.create_task(_safe_sostabo())
    meteo_task = _asyncio.create_task(get_meteo())
    live, meteo = await _asyncio.gather(live_task, meteo_task)

    eventi_attivi = get_active_and_soon(within_hours=2)
    ztl_st        = ztl_status()
    lezioni       = is_giorno_lezioni()
    esami         = is_sessione_esami()

    return compute_street_scores(
        _streets_geojson, live, [],
        eventi=eventi_attivi,
        pioggia=meteo.pioggia if meteo else False,
        unibo_lezioni=lezioni,
        unibo_esami=esami,
        ztl_attiva=ztl_st["attiva"],
        ztl_attiva_tra_30_min=ztl_st["attiva_tra_30_min"],
    )
