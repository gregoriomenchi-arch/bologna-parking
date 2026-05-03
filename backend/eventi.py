"""
Modulo eventi Bologna.

Fase attuale: eventi di test hardcoded, riseminati ad ogni avvio.
Fase futura:  refresh_eventi() chiamerà gli scraper reali (bolognfc.com, ecc.).
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from pydantic import BaseModel

FOOTBALL_DATA_KEY = os.environ.get("FOOTBALL_DATA_KEY", "")

from db import connect

log = logging.getLogger("bologna_parking.eventi")

# ---------------------------------------------------------------------------
# Venues con coordinate corrette e profilo d'impatto
# ---------------------------------------------------------------------------

VENUES: dict[str, dict] = {
    "Stadio Renato Dall'Ara": {
        "lat": 44.4929, "lon": 11.3097,
        "impatto": "alto",  "raggio_km": 1.5,
        "zona": "Dall'Ara / Saragozza",
    },
    "Fiera di Bologna": {
        "lat": 44.5289, "lon": 11.3647,
        "impatto": "alto",  "raggio_km": 2.0,
        "zona": "Fiera / San Donato",
    },
    "Unipol Arena": {
        "lat": 44.5089, "lon": 11.2789,
        "impatto": "alto",  "raggio_km": 1.5,
        "zona": "Casalecchio",
    },
    "Teatro Comunale": {
        "lat": 44.4938, "lon": 11.3426,
        "impatto": "medio", "raggio_km": 0.8,
        "zona": "Centro / Piazza Verdi",
    },
}


# ---------------------------------------------------------------------------
# Modello Pydantic
# ---------------------------------------------------------------------------

class Evento(BaseModel):
    id:          Optional[int]      = None
    nome:        str
    venue:       str
    data_inizio: datetime
    data_fine:   Optional[datetime] = None
    lat:         float
    lon:         float
    impatto:     str                        # "alto" | "medio"
    raggio_km:   float
    fonte:       Optional[str]      = None


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def init_events_db() -> None:
    with connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS eventi (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nome        TEXT NOT NULL,
                venue       TEXT NOT NULL,
                data_inizio TEXT NOT NULL,
                data_fine   TEXT,
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
                impatto     TEXT NOT NULL DEFAULT 'medio',
                raggio_km   REAL NOT NULL DEFAULT 1.0,
                fonte       TEXT,
                aggiornato  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_eventi_data ON eventi(data_inizio);
        """)


def _save_events(eventi: list[Evento]) -> None:
    """Inserisce eventi (upsert per nome+venue+data_inizio)."""
    now = datetime.now(timezone.utc).isoformat()
    with connect() as conn:
        for e in eventi:
            exists = conn.execute(
                "SELECT 1 FROM eventi WHERE nome=? AND venue=? AND data_inizio=?",
                (e.nome, e.venue, e.data_inizio.isoformat()),
            ).fetchone()
            if not exists:
                conn.execute(
                    """INSERT INTO eventi
                       (nome, venue, data_inizio, data_fine, lat, lon,
                        impatto, raggio_km, fonte, aggiornato)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        e.nome, e.venue,
                        e.data_inizio.isoformat(),
                        e.data_fine.isoformat() if e.data_fine else None,
                        e.lat, e.lon, e.impatto, e.raggio_km, e.fonte, now,
                    ),
                )


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def get_upcoming_events(hours: int = 48) -> list[dict]:
    """Restituisce gli eventi che iniziano nelle prossime `hours` ore."""
    now   = datetime.now(timezone.utc)
    until = now + timedelta(hours=hours)
    now_s   = now.isoformat()[:19]
    until_s = until.isoformat()[:19]
    with connect() as conn:
        rows = conn.execute(
            """SELECT id, nome, venue, data_inizio, data_fine,
                      lat, lon, impatto, raggio_km, fonte
               FROM eventi
               WHERE SUBSTR(data_inizio, 1, 19) BETWEEN ? AND ?
               ORDER BY data_inizio""",
            (now_s, until_s),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_active_and_soon(within_hours: int = 2) -> list[dict]:
    """
    Restituisce gli eventi:
    - attivi in questo momento (iniziati ma non ancora terminati), oppure
    - che iniziano entro `within_hours` ore.
    """
    now    = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=within_hours)
    now_s  = now.isoformat()[:19]
    cut_s  = cutoff.isoformat()[:19]
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, nome, venue, data_inizio, data_fine,
                   lat, lon, impatto, raggio_km, fonte
            FROM eventi
            WHERE
              (SUBSTR(data_inizio, 1, 19) <= ? AND (data_fine IS NULL OR SUBSTR(data_fine, 1, 19) >= ?))
              OR (SUBSTR(data_inizio, 1, 19) > ? AND SUBSTR(data_inizio, 1, 19) <= ?)
            ORDER BY data_inizio
            """,
            (now_s, now_s, now_s, cut_s),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(r: tuple) -> dict:
    return {
        "id": r[0], "nome": r[1], "venue": r[2],
        "data_inizio": r[3], "data_fine": r[4],
        "lat": r[5], "lon": r[6],
        "impatto": r[7], "raggio_km": r[8], "fonte": r[9],
    }


# ---------------------------------------------------------------------------
# Scraper reale — Bologna FC (football-data.org, free tier)
# ---------------------------------------------------------------------------

async def _fetch_bologna_fc() -> list[Evento]:
    """
    Scarica le prossime partite del Bologna FC da football-data.org.
    Team ID Bologna FC = 98, competizione Serie A = SA.
    Richiede FOOTBALL_DATA_KEY impostata come variabile d'ambiente.
    """
    if not FOOTBALL_DATA_KEY:
        log.warning("FOOTBALL_DATA_KEY non impostata")
        return []
    url = "https://api.football-data.org/v4/teams/84/matches"
    params = {"status": "SCHEDULED,LIVE", "limit": 10}
    venue = VENUES["Stadio Renato Dall'Ara"]
    eventi = []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params,
                                    headers={"X-Auth-Token": FOOTBALL_DATA_KEY})
            if resp.status_code != 200:
                log.warning("football-data.org: %s", resp.status_code)
                return []
            data = resp.json()
        for match in data.get("matches", []):
            utc_date = match.get("utcDate")
            if not utc_date:
                continue
            dt_start = datetime.fromisoformat(utc_date.replace("Z", "+00:00"))
            dt_fine = dt_start + timedelta(hours=2)
            home = match.get("homeTeam", {}).get("shortName", "")
            away = match.get("awayTeam", {}).get("shortName", "")
            nome = f"{home} vs {away}"
            # solo partite in casa
            if match.get("homeTeam", {}).get("id") != 84:
                continue
            eventi.append(Evento(
                nome=nome,
                venue="Stadio Renato Dall'Ara",
                data_inizio=dt_start,
                data_fine=dt_fine,
                lat=venue["lat"], lon=venue["lon"],
                impatto=venue["impatto"], raggio_km=venue["raggio_km"],
                fonte="football-data.org",
            ))
    except Exception as exc:
        log.warning("Bologna FC scraping error: %s", exc)
    return eventi


# ---------------------------------------------------------------------------
# Scraper reale — Fiera di Bologna (JSON API + fallback hardcoded)
# ---------------------------------------------------------------------------

# Aggiornare manualmente ogni stagione
FIERE_PROGRAMMATE = [
    {"nome": "Cersaie",    "start": "2026-09-22", "end": "2026-09-26"},
    {"nome": "Cosmoprof",  "start": "2026-03-20", "end": "2026-03-23"},
    {"nome": "Arte Fiera", "start": "2027-01-30", "end": "2027-02-02"},
    {"nome": "Motor Show", "start": "2026-12-04", "end": "2026-12-13"},
    {"nome": "Saie",       "start": "2026-10-08", "end": "2026-10-10"},
    {"nome": "Marca",      "start": "2027-01-21", "end": "2027-01-22"},
]


def _fiere_fallback() -> list[Evento]:
    venue = VENUES["Fiera di Bologna"]
    now = datetime.now(timezone.utc)
    eventi = []
    for f in FIERE_PROGRAMMATE:
        dt_start = datetime.fromisoformat(f["start"]).replace(tzinfo=timezone.utc)
        dt_fine  = datetime.fromisoformat(f["end"]).replace(hour=22, tzinfo=timezone.utc)
        if dt_fine < now:
            continue
        eventi.append(Evento(
            nome=f["nome"],
            venue="Fiera di Bologna",
            data_inizio=dt_start,
            data_fine=dt_fine,
            lat=venue["lat"], lon=venue["lon"],
            impatto=venue["impatto"], raggio_km=venue["raggio_km"],
            fonte="bolognafiere.it-manual",
        ))
    return eventi


async def _fetch_fiera_bologna() -> list[Evento]:
    """
    Prova l'endpoint JSON pubblico di BolognaFiere; se non risponde usa il
    fallback hardcoded FIERE_PROGRAMMATE.
    """
    url = "https://www.bolognafiere.it/it/api/manifestazioni.json"
    venue = VENUES["Fiera di Bologna"]
    now = datetime.now(timezone.utc)
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 Bologna Parking App"
            })
        if resp.status_code not in (200, 301, 302):
            log.warning("bolognafiere.it JSON: %s — uso fallback", resp.status_code)
            # Se l'API non esiste, usa direttamente il fallback
            raise Exception("API non disponibile, uso fallback")
        items = resp.json() if isinstance(resp.json(), list) else resp.json().get("manifestazioni", [])
        eventi = []
        for item in items:
            nome = (item.get("titolo") or item.get("nome") or "")[:80]
            start_raw = item.get("data_inizio") or item.get("start") or ""
            end_raw   = item.get("data_fine")   or item.get("end")   or ""
            if not nome or not start_raw:
                continue
            try:
                dt_start = datetime.fromisoformat(start_raw).replace(tzinfo=timezone.utc)
                dt_fine  = datetime.fromisoformat(end_raw).replace(tzinfo=timezone.utc) if end_raw else dt_start + timedelta(days=3)
            except ValueError:
                continue
            if dt_fine < now:
                continue
            eventi.append(Evento(
                nome=nome,
                venue="Fiera di Bologna",
                data_inizio=dt_start,
                data_fine=dt_fine,
                lat=venue["lat"], lon=venue["lon"],
                impatto=venue["impatto"], raggio_km=venue["raggio_km"],
                fonte="bolognafiere.it",
            ))
            if len(eventi) >= 10:
                break
        if not eventi:
            log.info("bolognafiere.it JSON vuoto — uso fallback")
            return _fiere_fallback()
        return eventi
    except Exception as exc:
        log.warning("Fiera Bologna scraping error: %s — uso fallback", exc)
        return _fiere_fallback()


# ---------------------------------------------------------------------------
# Refresh reale
# ---------------------------------------------------------------------------

async def refresh_eventi() -> int:
    """
    Scarica eventi reali da Bologna FC e Fiera Bologna.
    Ritorna il numero di nuovi eventi inseriti.
    """
    # Elimina eventi di test scaduti (fonte="test")
    now = datetime.now(timezone.utc)
    with connect() as conn:
        conn.execute(
            "DELETE FROM eventi WHERE fonte='test' AND data_fine < ?",
            (now.isoformat(),)
        )

    # Scraping reale
    bologna_fc = await _fetch_bologna_fc()
    fiera = await _fetch_fiera_bologna()
    nuovi = bologna_fc + fiera

    if nuovi:
        _save_events(nuovi)
        log.info("refresh_eventi: %d nuovi eventi (BFC=%d, Fiera=%d)",
                 len(nuovi), len(bologna_fc), len(fiera))

    return len(nuovi)
