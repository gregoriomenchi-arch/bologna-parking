"""
Bologna Parking API — Backend FastAPI
Fase 1: dati live SostaBo via portale Open Data del Comune di Bologna
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from sostabo import SostaBoClient, ParcheggioDisponibilita, ParcheggioStruttura, ParcheggioZona


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Al momento non servono risorse persistenti; il client HTTP
    # viene creato per ogni request (stateless e thread-safe).
    yield


app = FastAPI(
    title="Bologna Parking API",
    description="Dati in tempo reale sui parcheggi di Bologna via Open Data del Comune.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*",                      # copre tutto; in produzione restringere
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Sistema"])
async def health():
    """
    Verifica che il server sia in esecuzione e che il portale SostaBo
    (Open Data Bologna) sia raggiungibile.
    """
    async with SostaBoClient() as client:
        upstream_ok = await client.ping()

    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "upstream_sostabo": "reachable" if upstream_ok else "unreachable",
    }


@app.get(
    "/parcheggi/disponibilita",
    response_model=list[ParcheggioDisponibilita],
    tags=["Parcheggi"],
)
async def disponibilita(
    limit: int = Query(default=50, ge=1, le=200, description="Numero massimo di risultati"),
):
    """
    Restituisce la disponibilità in tempo reale di tutti i parcheggi di Bologna.
    I dati vengono aggiornati ogni ~10 minuti dal portale Open Data del Comune.
    """
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
    """
    Restituisce i parcheggi di Bologna da OpenStreetMap via Overpass API.
    Include strutture, parcheggi a raso, strisce blu e bianche.
    Fonte: OpenStreetMap contributors (ODbL).
    """
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
async def strutture(
    limit: int = Query(default=100, ge=1, le=500, description="Numero massimo di risultati"),
):
    """
    Restituisce l'elenco delle strutture di sosta (dati statici) di Bologna.
    """
    try:
        async with SostaBoClient() as client:
            return await client.get_strutture(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Errore upstream SostaBo: {exc}")
