"""
Fase 3 — storico e scoring strade.

Responsabilità:
  - Salva ogni lettura SostaBo in SQLite (readings).
  - Memorizza nella cache le geometrie strade da Overpass (streets_cache).
  - Calcola score 0-100 per ogni strada (alta probabilità di parcheggio = alto score).
"""

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from db import connect

# ZTL Bologna — Piazza Maggiore (penalità score per strade vicine)
_ZTL_LAT = 44.4938
_ZTL_LON = 11.3427

_BOLOGNA_BBOX   = "(44.44,11.27,44.56,11.44)"   # intero comune
_QUARTIERI_BBOX = "(44.47,11.29,44.52,11.40)"   # Saragozza/Murri/Irnerio/Bolognina/San Donato

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
    """Persiste una lista di ParcheggioDisponibilita nel DB."""
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
    """Media occupazione per parcheggio/ora/giorno — usato dall'endpoint /storico."""
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
    """Ritorna [(nome, avg_occ, lat, lon)] per l'ora e giorno della settimana correnti."""
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
    """Converte elementi Overpass (way con geom) in GeoJSON FeatureCollection."""
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
    """(lat, lon) del punto centrale di una LineString (coords = [lon, lat])."""
    mid = coords[len(coords) // 2]
    return mid[1], mid[0]


# ---------------------------------------------------------------------------
# Calcolo score strade
# ---------------------------------------------------------------------------

def _ztl_penalty(lat: float, lon: float) -> float:
    """Penalità 0–25: massima vicino alla ZTL, zero oltre 1.5 km."""
    dist = _haversine_km(lat, lon, _ZTL_LAT, _ZTL_LON)
    return max(0.0, 25.0 * (1.0 - dist / 1.5))


def compute_street_scores(
    streets_geojson: dict,
    live_parcheggi: list,           # list[ParcheggioDisponibilita]
    osm_zone: list,                 # list[ParcheggioZona]
    eventi: list | None = None,     # list[dict] da get_active_and_soon()
) -> dict:
    """
    Aggiunge la proprietà 'score' (0-100, più alto = più facile parcheggiare)
    a ogni feature della FeatureCollection e la ritorna.

    Score = f(occupazione real-time zona, storico ora corrente, ZTL, parcheggi OSM vicini)
    """
    storico = _storico_ora_corrente()  # [(nome, avg_occ, lat, lon)]

    live_pts = [
        (p.coordinate.lat, p.coordinate.lon, p.occupazione_pct, p.nome)
        for p in live_parcheggi if p.coordinate
    ]
    osm_pts = [
        (p.coordinate.lat, p.coordinate.lon)
        for p in osm_zone if p.coordinate
    ]

    scored = []
    for feat in streets_geojson.get("features", []):
        coords = feat["geometry"]["coordinates"]
        if not coords:
            continue
        lat, lon = _midpoint(coords)

        # 1. Parcheggi SostaBo live entro 1 km
        nearby_live = [(occ, nome) for (plat, plon, occ, nome) in live_pts
                       if _haversine_km(lat, lon, plat, plon) < 1.0]
        nearby_occ   = [occ  for occ, _    in nearby_live]
        nearby_names = {nome for _,    nome in nearby_live}

        # 2. Storico ora corrente — parcheggi entro 1 km per coordinate
        #    (indipendente dai live, così funziona anche senza SostaBo attivo)
        nearby_hist = [occ for (nome, occ, plat, plon) in storico
                       if _haversine_km(lat, lon, plat, plon) < 1.0]
        # Se abbiamo anche live, preferisci storico dei parcheggi già trovati
        if nearby_names:
            hist_by_name = [occ for (nome, occ, _, __) in storico if nome in nearby_names]
            if hist_by_name:
                nearby_hist = hist_by_name

        if nearby_occ:
            rt = 100.0 - (sum(nearby_occ) / len(nearby_occ))
        elif nearby_hist:
            rt = 100.0 - (sum(nearby_hist) / len(nearby_hist))
        else:
            rt = 50.0  # default neutro se nessun dato nelle vicinanze

        # 3. Blend real-time + storico (60/40) se entrambi disponibili
        if nearby_occ and nearby_hist:
            hist = 100.0 - (sum(nearby_hist) / len(nearby_hist))
            rt = 0.6 * rt + 0.4 * hist

        # 4. Penalità ZTL
        score = rt - _ztl_penalty(lat, lon)

        # 5. Bonus parcheggi OSM entro 300 m (max +15)
        osm_n = sum(1 for (olat, olon) in osm_pts
                    if _haversine_km(lat, lon, olat, olon) < 0.3)
        score += min(15.0, osm_n * 3.0)

        # 6. Penalità eventi attivi o imminenti (entro raggio evento)
        if eventi:
            now = datetime.now(timezone.utc)
            for ev in eventi:
                ev_lat = ev.get("lat")
                ev_lon = ev.get("lon")
                if ev_lat is None or ev_lon is None:
                    continue
                if _haversine_km(lat, lon, ev_lat, ev_lon) >= ev.get("raggio_km", 1.0):
                    continue
                try:
                    dt_start = datetime.fromisoformat(ev["data_inizio"])
                    dt_end   = (datetime.fromisoformat(ev["data_fine"])
                                if ev.get("data_fine") else dt_start + timedelta(hours=2))
                    if dt_start.tzinfo is None:
                        dt_start = dt_start.replace(tzinfo=timezone.utc)
                    if dt_end.tzinfo is None:
                        dt_end = dt_end.replace(tzinfo=timezone.utc)
                    if dt_start <= now <= dt_end:
                        factor = 0.50   # evento attivo: -50%
                    elif now < dt_start <= now + timedelta(hours=2):
                        factor = 0.30   # inizia a breve: -30%
                    else:
                        continue
                except (KeyError, ValueError):
                    factor = 0.30
                if ev.get("impatto") != "alto":
                    factor *= 0.6   # impatto medio: penalità ridotta
                score = score * (1.0 - factor)

        score = round(max(0.0, min(100.0, score)), 1)

        props = {**feat["properties"], "score": score}
        scored.append({"type": "Feature", "geometry": feat["geometry"], "properties": props})

    return {"type": "FeatureCollection", "features": scored}
