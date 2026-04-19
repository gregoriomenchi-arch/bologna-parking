"""
Modulo eventi Bologna.

Fase attuale: eventi di test hardcoded, riseminati ad ogni avvio.
Fase futura:  refresh_eventi() chiamerà gli scraper reali (bolognfc.com, ecc.).
"""

import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

log = logging.getLogger("bologna_parking.eventi")
DB_PATH = Path(__file__).parent / "parking_history.db"

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
    with sqlite3.connect(DB_PATH) as conn:
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
    with sqlite3.connect(DB_PATH) as conn:
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
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT id, nome, venue, data_inizio, data_fine,
                      lat, lon, impatto, raggio_km, fonte
               FROM eventi
               WHERE data_inizio BETWEEN ? AND ?
               ORDER BY data_inizio""",
            (now.isoformat(), until.isoformat()),
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
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT id, nome, venue, data_inizio, data_fine,
                   lat, lon, impatto, raggio_km, fonte
            FROM eventi
            WHERE
              (data_inizio <= :now AND (data_fine IS NULL OR data_fine >= :now))
              OR (data_inizio > :now AND data_inizio <= :cutoff)
            ORDER BY data_inizio
            """,
            {"now": now.isoformat(), "cutoff": cutoff.isoformat()},
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
# Seed eventi di test (hardcoded, relativi a "adesso")
# ---------------------------------------------------------------------------

def seed_test_events() -> None:
    """
    Inserisce eventi realistici con date relative a now.
    Viene chiamato ad ogni avvio; il controllo upsert evita duplicati.

    Scenario di test:
      - Bologna FC vs Inter  →  partita in corso (iniziata 30 min fa)
      - Fiera Cersaie        →  inizia tra 90 minuti
      - Concerto Unipol Arena →  domani sera 21:00
      - Teatro Comunale       →  dopodomani sera 20:30
    """
    now = datetime.now(timezone.utc)

    # Arrotonda all'ora più vicina per avere date "pulite"
    base = now.replace(minute=0, second=0, microsecond=0)

    v_dallara   = VENUES["Stadio Renato Dall'Ara"]
    v_fiera     = VENUES["Fiera di Bologna"]
    v_unipol    = VENUES["Unipol Arena"]
    v_teatro    = VENUES["Teatro Comunale"]

    test_events = [
        # Partita in corso (banner "in corso")
        Evento(
            nome="Bologna FC vs Inter",
            venue="Stadio Renato Dall'Ara",
            data_inizio=base - timedelta(minutes=30),
            data_fine=base + timedelta(minutes=90),
            lat=v_dallara["lat"], lon=v_dallara["lon"],
            impatto=v_dallara["impatto"], raggio_km=v_dallara["raggio_km"],
            fonte="test",
        ),
        # Fiera imminente (banner "alle HH:MM")
        Evento(
            nome="Cersaie — Salone Ceramica",
            venue="Fiera di Bologna",
            data_inizio=base + timedelta(hours=1, minutes=30),
            data_fine=base + timedelta(hours=1, minutes=30) + timedelta(days=4),
            lat=v_fiera["lat"], lon=v_fiera["lon"],
            impatto=v_fiera["impatto"], raggio_km=v_fiera["raggio_km"],
            fonte="test",
        ),
        # Concerto domani sera
        Evento(
            nome="Vasco Rossi Live",
            venue="Unipol Arena",
            data_inizio=(base + timedelta(days=1)).replace(hour=21),
            data_fine=(base + timedelta(days=1)).replace(hour=23, minute=30),
            lat=v_unipol["lat"], lon=v_unipol["lon"],
            impatto=v_unipol["impatto"], raggio_km=v_unipol["raggio_km"],
            fonte="test",
        ),
        # Spettacolo teatro dopodomani
        Evento(
            nome="Don Giovanni — Mozart",
            venue="Teatro Comunale",
            data_inizio=(base + timedelta(days=2)).replace(hour=20, minute=30),
            data_fine=(base + timedelta(days=2)).replace(hour=23),
            lat=v_teatro["lat"], lon=v_teatro["lon"],
            impatto=v_teatro["impatto"], raggio_km=v_teatro["raggio_km"],
            fonte="test",
        ),
    ]

    _save_events(test_events)
    log.info("Seed eventi di test: %d eventi caricati", len(test_events))


# ---------------------------------------------------------------------------
# Stub per futura integrazione scraping reale
# ---------------------------------------------------------------------------

async def refresh_eventi() -> int:
    """
    Placeholder per scraping reale (bolognfc.com, bolognafiere.it, ecc.).
    Attualmente è un no-op; ritorna 0 nuovi eventi.
    """
    log.debug("refresh_eventi: scraping non ancora implementato")
    return 0
