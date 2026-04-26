"""
Storico e scoring strade.

Responsabilità:
  - Salva ogni lettura SostaBo in PostgreSQL (tabella readings).
  - Calcola score 0-100 per ogni strada (più alto = più facile parcheggiare).

Algoritmo score (nuovi predittori reali):
  base     = media storica ora corrente per parcheggi vicini (fallback 50)
  pioggia  → -20
  lezioni  → -15 (solo strade in zona universitaria)
  esami    → -25 (solo strade in zona universitaria)
  evento   → -30 (entro raggio evento, evento attivo)
  ztl_presto → -10 (strade nel buffer ZTL, ZTL attiva entro 30 min)
  clamp [0, 100]
"""

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from db import connect
from unibo import is_zona_universitaria
from ztl import is_nel_buffer_ztl

log = logging.getLogger("bologna_parking.score")

_BOLOGNA_BBOX   = "(44.44,11.27,44.56,11.44)"
_QUARTIERI_BBOX = "(44.47,11.29,44.52,11.40)"

OVERPASS_STREETS_QUERY = f"""[out:json][timeout:60];
(
  way["highway"~"^(primary|secondary|tertiary)$"]["name"]{_BOLOGNA_BBOX};
  way["highway"~"^(residential|living_street)$"]["name"]{_QUARTIERI_BBOX};
);
out geom;"""


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

def init_db() -> None:
    with connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS readings (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp        TEXT    NOT NULL,
                ora              INTEGER NOT NULL,
                giorno_settimana INTEGER NOT NULL,
                parcheggio_nome  TEXT    NOT NULL,
                posti_liberi     INTEGER,
                posti_occupati   INTEGER,
                posti_totali     INTEGER,
                occupazione_pct  REAL,
                lat              REAL,
                lon              REAL
            );
            CREATE INDEX IF NOT EXISTS idx_readings_lookup
                ON readings(parcheggio_nome, ora, giorno_settimana);

            CREATE TABLE IF NOT EXISTS streets_cache (
                id         INTEGER PRIMARY KEY,
                fetched_at TEXT NOT NULL,
                geojson    TEXT NOT NULL
            );
        """)


# ---------------------------------------------------------------------------
# Salvataggio letture SostaBo
# ---------------------------------------------------------------------------

def save_readings(parcheggi: list) -> None:
    now = datetime.now(timezone.utc)
    rows = [
        (
            now.isoformat(), now.hour, now.weekday(),
            p.nome, p.posti_liberi, p.posti_occupati, p.posti_totali,
            p.occupazione_pct,
            p.coordinate.lat if p.coordinate else None,
            p.coordinate.lon if p.coordinate else None,
        )
        for p in parcheggi
    ]
    with connect() as conn:
        conn.executemany(
            """INSERT INTO readings
               (timestamp, ora, giorno_settimana, parcheggio_nome,
                posti_liberi, posti_occupati, posti_totali, occupazione_pct, lat, lon)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )


# ---------------------------------------------------------------------------
# Query storico
# ---------------------------------------------------------------------------

def get_storico() -> list[dict]:
    with connect() as conn:
        rows = conn.execute("""
            SELECT parcheggio_nome, ora, giorno_settimana,
                   ROUND(AVG(occupazione_pct), 1) AS avg_occ,
                   COUNT(*) AS campioni,
                   AVG(lat) AS lat, AVG(lon) AS lon
            FROM readings
            GROUP BY parcheggio_nome, ora, giorno_settimana
            ORDER BY parcheggio_nome, giorno_settimana, ora
        """).fetchall()
    return [
        {"nome": r[0], "ora": r[1], "giorno": r[2],
         "avg_occupazione": r[3], "campioni": r[4], "lat": r[5], "lon": r[6]}
        for r in rows
    ]


def _storico_ora_corrente() -> list[tuple[str, float, float, float]]:
    """[(nome, avg_occ, lat, lon)] per ora e giorno della settimana correnti."""
    now = datetime.now(timezone.utc)
    with connect() as conn:
        rows = conn.execute("""
            SELECT parcheggio_nome, AVG(occupazione_pct), AVG(lat), AVG(lon)
            FROM readings
            WHERE ora = ? AND giorno_settimana = ?
              AND lat IS NOT NULL AND lon IS NOT NULL
            GROUP BY parcheggio_nome
        """, (now.hour, now.weekday())).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


# ---------------------------------------------------------------------------
# Cache strade Overpass
# ---------------------------------------------------------------------------

def load_streets_cache() -> Optional[dict]:
    with connect() as conn:
        row = conn.execute(
            "SELECT geojson FROM streets_cache ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return json.loads(row[0]) if row else None


def save_streets_cache(geojson: dict) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM streets_cache")
        conn.execute(
            "INSERT INTO streets_cache (fetched_at, geojson) VALUES (?, ?)",
            (datetime.now(timezone.utc).isoformat(), json.dumps(geojson)),
        )


def overpass_elements_to_geojson(elements: list) -> dict:
    features = []
    for el in elements:
        geom = el.get("geometry", [])
        if len(geom) < 2:
            continue
        coords = [[pt["lon"], pt["lat"]] for pt in geom]
        tags = el.get("tags", {})
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "name":    tags.get("name", ""),
                "highway": tags.get("highway", ""),
                "osm_id":  el["id"],
            },
        })
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Geometria helpers
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _midpoint(coords: list) -> tuple[float, float]:
    """(lat, lon) del punto centrale di una LineString (coords = [[lon, lat], ...])."""
    mid = coords[len(coords) // 2]
    return mid[1], mid[0]


# ---------------------------------------------------------------------------
# Scoring engine — cuore del nuovo algoritmo
# ---------------------------------------------------------------------------

def _base_score(
    lat: float, lon: float,
    live_pts: list[tuple[float, float, float, str]],
    storico: list[tuple[str, float, float, float]],
) -> tuple[float, int, int]:
    """
    Calcola la componente base dello score da dati real-time + storico.
    Ritorna (base_score, n_live_vicini, n_storico_vicini).
    """
    nearby_live = [(occ, nome) for (plat, plon, occ, nome) in live_pts
                   if _haversine_km(lat, lon, plat, plon) < 1.0]
    nearby_names = {nome for _, nome in nearby_live}
    nearby_occ   = [occ  for occ, _  in nearby_live]

    # Storico: preferisci i parcheggi già trovati nel live, altrimenti per coordinate
    if nearby_names:
        nearby_hist = [occ for (nome, occ, _, __) in storico if nome in nearby_names]
    else:
        nearby_hist = [occ for (_, occ, plat, plon) in storico
                       if _haversine_km(lat, lon, plat, plon) < 1.0]

    if nearby_occ and nearby_hist:
        rt = 100.0 - (sum(nearby_occ) / len(nearby_occ))
        hs = 100.0 - (sum(nearby_hist) / len(nearby_hist))
        base = 0.6 * rt + 0.4 * hs   # 60% real-time, 40% storico
    elif nearby_occ:
        base = 100.0 - (sum(nearby_occ) / len(nearby_occ))
    elif nearby_hist:
        base = 100.0 - (sum(nearby_hist) / len(nearby_hist))
    else:
        base = 50.0   # nessun dato nelle vicinanze

    return base, len(nearby_occ), len(nearby_hist)


def _compute_score(
    lat: float,
    lon: float,
    name: str,
    live_pts: list[tuple[float, float, float, str]],
    storico: list[tuple[str, float, float, float]],
    eventi: list[dict] | None,
    *,
    pioggia: bool = False,
    unibo_lezioni: bool = False,
    unibo_esami: bool = False,
    ztl_attiva_tra_30_min: bool = False,
) -> tuple[float, dict]:
    """
    Calcola score (0-100) e breakdown dei fattori per una singola strada.
    Ritorna (score, factors) dove factors è un dict per il debug.
    """
    base, n_live, n_hist = _base_score(lat, lon, live_pts, storico)
    factors: dict[str, float] = {"base": round(base, 1)}
    score = base

    # Pioggia → -20 (tutti trovano parcheggio più difficile)
    if pioggia:
        score -= 20.0
        factors["pioggia"] = -20.0

    # Calendario UniBo — solo se la strada è in zona universitaria
    in_unibo = is_zona_universitaria(lat, lon)
    if in_unibo:
        if unibo_esami:
            score -= 25.0
            factors["sessione_esami"] = -25.0
        elif unibo_lezioni:
            score -= 15.0
            factors["giorno_lezioni"] = -15.0

    # ZTL buffer — strade fuori ZTL che risentono dell'effetto pendolare pre-attivazione
    if ztl_attiva_tra_30_min and is_nel_buffer_ztl(lat, lon):
        score -= 10.0
        factors["ztl_buffer_presto"] = -10.0

    # Eventi attivi nelle vicinanze → -30
    if eventi:
        now = datetime.now(timezone.utc)
        for ev in eventi:
            ev_lat = ev.get("lat")
            ev_lon = ev.get("lon")
            if ev_lat is None or ev_lon is None:
                continue
            raggio = ev.get("raggio_km", 1.0)
            if _haversine_km(lat, lon, ev_lat, ev_lon) >= raggio:
                continue
            try:
                dt_start = datetime.fromisoformat(ev["data_inizio"])
                dt_end   = (datetime.fromisoformat(ev["data_fine"])
                            if ev.get("data_fine") else dt_start + timedelta(hours=2))
                if dt_start.tzinfo is None:
                    dt_start = dt_start.replace(tzinfo=timezone.utc)
                if dt_end.tzinfo is None:
                    dt_end = dt_end.replace(tzinfo=timezone.utc)
                if not (dt_start <= now <= dt_end):
                    continue
            except (KeyError, ValueError):
                continue
            penalty = -30.0 if ev.get("impatto") == "alto" else -18.0
            score += penalty
            factors[f"evento_{ev.get('nome', 'sconosciuto')[:20]}"] = penalty

    score = round(max(0.0, min(100.0, score)), 1)

    if score < 25.0:
        log.debug("Score basso [%.0f] — %s  fattori: %s", score, name, factors)

    return score, factors


# ---------------------------------------------------------------------------
# API pubblica
# ---------------------------------------------------------------------------

def compute_street_scores(
    streets_geojson: dict,
    live_parcheggi: list,
    osm_zone: list,             # non più usato, mantenuto per compatibilità API
    eventi: list | None = None,
    *,
    pioggia: bool = False,
    unibo_lezioni: bool = False,
    unibo_esami: bool = False,
    ztl_attiva: bool = False,
    ztl_attiva_tra_30_min: bool = False,
) -> dict:
    """
    Aggiunge la proprietà 'score' (0-100) a ogni feature del GeoJSON e la ritorna.
    Parametri keyword-only per i predittori reali (tutti con default False = backward compat).
    """
    storico = _storico_ora_corrente()
    live_pts = [
        (p.coordinate.lat, p.coordinate.lon, p.occupazione_pct, p.nome)
        for p in live_parcheggi if p.coordinate
    ]

    scored = []
    for feat in streets_geojson.get("features", []):
        coords = feat["geometry"]["coordinates"]
        if not coords:
            continue
        lat, lon = _midpoint(coords)
        name = feat["properties"].get("name", "")

        score, _ = _compute_score(
            lat, lon, name, live_pts, storico, eventi,
            pioggia=pioggia,
            unibo_lezioni=unibo_lezioni,
            unibo_esami=unibo_esami,
            ztl_attiva_tra_30_min=ztl_attiva_tra_30_min,
        )

        props = {**feat["properties"], "score": score}
        scored.append({"type": "Feature", "geometry": feat["geometry"], "properties": props})

    return {"type": "FeatureCollection", "features": scored}


def compute_single_street_score(
    name_query: str,
    streets_geojson: dict,
    live_parcheggi: list,
    eventi: list | None = None,
    *,
    pioggia: bool = False,
    unibo_lezioni: bool = False,
    unibo_esami: bool = False,
    ztl_attiva_tra_30_min: bool = False,
) -> dict | None:
    """
    Trova la prima strada con nome ~= name_query e ritorna il breakdown completo.
    Usato dall'endpoint /debug/score/{via}.
    Ritorna None se la strada non è trovata.
    """
    query_low = name_query.lower()
    match = None
    for feat in streets_geojson.get("features", []):
        nome = feat["properties"].get("name", "")
        if query_low in nome.lower():
            match = feat
            break
    if match is None:
        return None

    coords = match["geometry"]["coordinates"]
    lat, lon = _midpoint(coords)
    nome = match["properties"].get("name", "")

    storico = _storico_ora_corrente()
    live_pts = [
        (p.coordinate.lat, p.coordinate.lon, p.occupazione_pct, p.nome)
        for p in live_parcheggi if p.coordinate
    ]

    score, factors = _compute_score(
        lat, lon, nome, live_pts, storico, eventi,
        pioggia=pioggia,
        unibo_lezioni=unibo_lezioni,
        unibo_esami=unibo_esami,
        ztl_attiva_tra_30_min=ztl_attiva_tra_30_min,
    )

    from unibo import is_zona_universitaria
    from ztl import is_in_ztl, is_nel_buffer_ztl

    return {
        "via": nome,
        "lat": lat,
        "lon": lon,
        "score_finale": score,
        "fattori": factors,
        "contesto": {
            "in_zona_unibo": is_zona_universitaria(lat, lon),
            "in_ztl": is_in_ztl(lat, lon),
            "nel_buffer_ztl": is_nel_buffer_ztl(lat, lon),
        },
        "predittori_attivi": {
            "pioggia": pioggia,
            "unibo_lezioni": unibo_lezioni,
            "unibo_esami": unibo_esami,
            "ztl_attiva_tra_30_min": ztl_attiva_tra_30_min,
        },
    }
