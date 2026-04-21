"""
Raccolta dati traffico via TomTom Flow Segment API.

Richiede la variabile d'ambiente TOMTOM_KEY.
Se assente, la raccolta è silenziosamente disabilitata.

Tabella DB: traffic_readings
"""

import logging
import math
import os
from datetime import datetime, timedelta, timezone

import httpx

from db import connect

log = logging.getLogger("bologna_parking.traffic")

TOMTOM_KEY = os.environ.get("TOMTOM_KEY", "")
_TOMTOM_URL = (
    "https://api.tomtom.com/traffic/services/4"
    "/flowSegmentData/relative0/10/json"
)

# ---------------------------------------------------------------------------
# Punti di monitoraggio — strade principali Bologna
# ---------------------------------------------------------------------------

MONITORED_POINTS: list[dict] = [
    {"strada": "Via dell'Indipendenza",  "lat": 44.4978, "lon": 11.3424},
    {"strada": "Via Stalingrado",         "lat": 44.5123, "lon": 11.3456},
    {"strada": "Viale Pietramellara",     "lat": 44.5068, "lon": 11.3423},
    {"strada": "Via Andrea Costa",        "lat": 44.4921, "lon": 11.3106},
    {"strada": "Via Saragozza",           "lat": 44.4912, "lon": 11.3203},
    {"strada": "Via Emilia Levante",      "lat": 44.4912, "lon": 11.3634},
    {"strada": "Via Felsina",             "lat": 44.4834, "lon": 11.3678},
    {"strada": "Via Castiglione",         "lat": 44.4887, "lon": 11.3512},
    {"strada": "Via San Donato",          "lat": 44.5134, "lon": 11.3701},
    {"strada": "Via del Lavoro",          "lat": 44.5156, "lon": 11.3234},
    {"strada": "Tangenziale Nord",        "lat": 44.5289, "lon": 11.3345},
    {"strada": "Via Panzacchi",           "lat": 44.4901, "lon": 11.3101},
]


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def init_traffic_db() -> None:
    with connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS traffic_readings (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp         TEXT    NOT NULL,
                ora               INTEGER NOT NULL,
                giorno_settimana  INTEGER NOT NULL,
                strada            TEXT    NOT NULL,
                lat               REAL    NOT NULL,
                lon               REAL    NOT NULL,
                velocita_attuale  REAL,
                velocita_libera   REAL,
                congestione       INTEGER,
                evento_attivo     INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_traffic_lookup
                ON traffic_readings(strada, ora, giorno_settimana);
            CREATE INDEX IF NOT EXISTS idx_traffic_ts
                ON traffic_readings(timestamp)
        """)


# ---------------------------------------------------------------------------
# Raccolta TomTom
# ---------------------------------------------------------------------------

async def collect_traffic(eventi_attivi: list[dict]) -> int:
    """
    Chiama TomTom Flow per ogni punto monitorato e salva in DB.
    Ritorna il numero di record salvati (0 se TOMTOM_KEY non è impostata).
    """
    if not TOMTOM_KEY:
        return 0

    ev_flag = 1 if eventi_attivi else 0
    now = datetime.now(timezone.utc)
    rows: list[tuple] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for pt in MONITORED_POINTS:
            try:
                resp = await client.get(
                    _TOMTOM_URL,
                    params={
                        "point": f"{pt['lat']},{pt['lon']}",
                        "unit": "KMPH",
                        "key": TOMTOM_KEY,
                    },
                )
                resp.raise_for_status()
                seg = resp.json().get("flowSegmentData", {})

                v_attuale = seg.get("currentSpeed")
                v_libera  = seg.get("freeFlowSpeed")
                if v_attuale and v_libera and v_libera > 0:
                    cong = round(max(0, min(100, 100 * (1 - v_attuale / v_libera))))
                else:
                    cong = None

                rows.append((
                    now.isoformat(), now.hour, now.weekday(),
                    pt["strada"], pt["lat"], pt["lon"],
                    v_attuale, v_libera, cong, ev_flag,
                ))
            except Exception as exc:
                log.debug("TomTom %s: %s", pt["strada"], exc)

    if rows:
        with connect() as conn:
            conn.executemany(
                """INSERT INTO traffic_readings
                   (timestamp, ora, giorno_settimana, strada, lat, lon,
                    velocita_attuale, velocita_libera, congestione, evento_attivo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
        log.info("Traffico: salvati %d punti (evento_attivo=%d)", len(rows), ev_flag)

    return len(rows)


# ---------------------------------------------------------------------------
# Query storico
# ---------------------------------------------------------------------------

def get_storico_traffico() -> list[dict]:
    """Media velocità/congestione per strada, ora e giorno della settimana."""
    with connect() as conn:
        rows = conn.execute("""
            SELECT strada,
                   ora,
                   giorno_settimana,
                   ROUND(AVG(velocita_attuale), 1) AS v_media,
                   ROUND(AVG(velocita_libera),  1) AS v_libera_media,
                   ROUND(AVG(congestione),       1) AS cong_media,
                   COUNT(*)                          AS campioni
            FROM traffic_readings
            WHERE congestione IS NOT NULL
            GROUP BY strada, ora, giorno_settimana
            ORDER BY strada, giorno_settimana, ora
        """).fetchall()
    return [
        {
            "strada": r[0], "ora": r[1], "giorno": r[2],
            "velocita_media": r[3], "velocita_libera_media": r[4],
            "congestione_media": r[5], "campioni": r[6],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Correlazioni eventi ↔ traffico
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + (
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def _tipo_evento(nome: str) -> str:
    n = nome.lower()
    if any(k in n for k in ("vs", " fc", "calcio", "partita", "serie a", "bologna fc")):
        return "partita"
    if any(k in n for k in ("fiera", "salone", "expo", "cersaie", "cosmoprof")):
        return "fiera"
    if any(k in n for k in ("concerto", "live", "tour", "festival")):
        return "concerto"
    if any(k in n for k in ("teatro", "opera", "spettacolo", "mozart", "verdi")):
        return "teatro"
    return "altro"


def _avg(vals: list) -> float | None:
    v = [x for x in vals if x is not None]
    return round(sum(v) / len(v), 1) if v else None


def get_correlazioni_eventi() -> list[dict]:
    """
    Per ogni tipo di evento (partita, fiera, concerto, teatro):
    ritorna l'impatto medio sulla congestione nelle 3 finestre temporali:
      - prima:   da -3h a data_inizio
      - durante: da data_inizio a data_fine
      - dopo:    da data_fine a data_fine+2h

    Considera solo le strade entro il raggio dell'evento.
    Richiede dati storici in traffic_readings.
    """
    # Carica tutti gli eventi
    with connect() as conn:
        ev_rows = conn.execute(
            "SELECT id, nome, lat, lon, raggio_km, data_inizio, data_fine FROM eventi"
        ).fetchall()

    if not ev_rows:
        return []

    # Accumula congestioni per tipo e finestra
    # tipo → { "prima": [...], "durante": [...], "dopo": [...], "n_eventi": int }
    buckets: dict[str, dict] = {}

    for ev in ev_rows:
        ev_id, nome, ev_lat, ev_lon, raggio, start_str, end_str = ev
        tipo = _tipo_evento(nome)

        try:
            dt_start = datetime.fromisoformat(start_str)
            if dt_start.tzinfo is None:
                dt_start = dt_start.replace(tzinfo=timezone.utc)
            dt_end = (
                datetime.fromisoformat(end_str)
                if end_str
                else dt_start + timedelta(hours=2)
            )
            if dt_end.tzinfo is None:
                dt_end = dt_end.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        window_start = (dt_start - timedelta(hours=3)).isoformat()
        window_end   = (dt_end   + timedelta(hours=2)).isoformat()

        # Leggi letture traffico nel periodo totale
        with connect() as conn:
            t_rows = conn.execute(
                """SELECT timestamp, lat, lon, congestione
                   FROM traffic_readings
                   WHERE timestamp BETWEEN ? AND ?
                     AND congestione IS NOT NULL""",
                (window_start, window_end),
            ).fetchall()

        # Filtra per distanza e separa per finestra
        prima, durante, dopo = [], [], []
        for ts_str, t_lat, t_lon, cong in t_rows:
            if _haversine_km(ev_lat, ev_lon, t_lat, t_lon) > raggio:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            if ts < dt_start:
                prima.append(cong)
            elif ts <= dt_end:
                durante.append(cong)
            else:
                dopo.append(cong)

        # Salta eventi senza dati traffico correlati
        if not (prima or durante or dopo):
            continue

        if tipo not in buckets:
            buckets[tipo] = {"prima": [], "durante": [], "dopo": [], "n_eventi": 0}
        buckets[tipo]["prima"].extend(prima)
        buckets[tipo]["durante"].extend(durante)
        buckets[tipo]["dopo"].extend(dopo)
        buckets[tipo]["n_eventi"] += 1

    return [
        {
            "tipo": tipo,
            "n_eventi": b["n_eventi"],
            "congestione_media": {
                "prima":   _avg(b["prima"]),
                "durante": _avg(b["durante"]),
                "dopo":    _avg(b["dopo"]),
            },
            "campioni": {
                "prima":   len(b["prima"]),
                "durante": len(b["durante"]),
                "dopo":    len(b["dopo"]),
            },
        }
        for tipo, b in sorted(buckets.items())
    ]
