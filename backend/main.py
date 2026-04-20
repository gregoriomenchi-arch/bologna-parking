"""
Bologna Parking API — Backend FastAPI
"""

import asyncio
import json
import logging
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
)
from eventi import (
    init_events_db,
    seed_test_events,
    get_upcoming_events,
    get_active_and_soon,
)

log = logging.getLogger("bologna_parking")

STATIC_STREETS = Path(__file__).parent / "data" / "strade_bologna.json"

# ---------------------------------------------------------------------------
# Stato in-memory
# ---------------------------------------------------------------------------

_streets_geojson: dict | None = None   # GeoJSON base caricato da file statico
_streets_ready = False


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _collect_loop() -> None:
    """Raccoglie dati SostaBo ogni 10 minuti e li salva in SQLite."""
    while True:
        try:
            async with SostaBoClient() as client:
                parcheggi = await client.get_disponibilita()
            save_readings(parcheggi)
            log.info("Raccolti %d record SostaBo", len(parcheggi))
        except Exception as exc:
            log.warning("Raccolta SostaBo fallita: %s", exc)
        await asyncio.sleep(600)



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

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_events_db()
    seed_test_events()               # eventi hardcoded per testing immediato
    _load_static_streets()           # sincrono, <50 ms, nessuna chiamata esterna
    asyncio.create_task(_collect_loop())
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


@app.get("/strade/probabilita", tags=["Strade"])
async def probabilita_strade():
    """
    GeoJSON con score aggiornati in tempo reale (dati SostaBo + storico SQLite).
    Carica la geometria dal file statico e ricalcola gli score.
    """
    if not _streets_ready or not _streets_geojson or not _streets_geojson["features"]:
        raise HTTPException(status_code=503, detail="Strade non disponibili")

    try:
        async with SostaBoClient() as client:
            live = await client.get_disponibilita()
    except Exception:
        live = []

    eventi_attivi = get_active_and_soon(within_hours=2)
    return compute_street_scores(_streets_geojson, live, [], eventi=eventi_attivi)
