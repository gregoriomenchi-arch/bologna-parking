"""
Raccolta una-tantum dati OSM per Bologna via Overpass API.

Categorie raccolte:
  - parking_spots   : strisce blu e parcheggi a pagamento su strada
  - parking_lots    : strutture (multi-piano, sotterranee, parcheggi di scambio)
  - ztl_zones       : zone a traffico limitato con poligoni e orari
  - pedestrian_areas: aree pedonali (impediscono transito veicolare)

Idempotente: non ri-esegue se la tabella osm_collections ha già righe
(a meno che collect_osm_data sia chiamata con force=True).
"""

import json
import logging
from datetime import datetime, timezone

import httpx

from db import connect

log = logging.getLogger("bologna_parking.osm")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BBOX = "44.44,11.27,44.56,11.44"       # intero comune di Bologna

# ---------------------------------------------------------------------------
# Query Overpass
# ---------------------------------------------------------------------------

_Q_PARKING_SPOTS = f"""[out:json][timeout:90];
(
  node["amenity"="parking"]["fee"="yes"]({BBOX});
  way["amenity"="parking"]["fee"="yes"]({BBOX});
  node["amenity"="parking"]["parking"="street_side"]({BBOX});
  way["amenity"="parking"]["parking"="street_side"]({BBOX});
  node["parking:lane:both"="parallel"]["parking:lane:both:fee"="yes"]({BBOX});
  way["parking:lane:both"="parallel"]["parking:lane:both:fee"="yes"]({BBOX});
);
out center tags;"""

_Q_PARKING_LOTS = f"""[out:json][timeout:90];
(
  node["amenity"="parking"]["parking"~"multi-storey|underground"]({BBOX});
  way["amenity"="parking"]["parking"~"multi-storey|underground"]({BBOX});
  node["amenity"="parking"]["access"~"^(yes|customers)$"]({BBOX});
  way["amenity"="parking"]["access"~"^(yes|customers)$"]({BBOX});
);
out center tags;"""

_Q_ZTL = f"""[out:json][timeout:90];
(
  relation["boundary"="restricted_area"]({BBOX});
  way["boundary"="restricted_area"]({BBOX});
  way["motor_vehicle"="no"]["access"="no"]({BBOX});
  way["access"="no"]["highway"~"residential|tertiary|secondary"]({BBOX});
);
out geom tags;"""

_Q_PEDESTRIAN = f"""[out:json][timeout:90];
(
  way["highway"="pedestrian"]["area"="yes"]({BBOX});
  way["highway"="pedestrian"]["area"!="yes"]({BBOX});
  relation["highway"="pedestrian"]({BBOX});
  way["foot"="designated"]["motor_vehicle"="no"]["area"="yes"]({BBOX});
);
out geom tags;"""


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

def init_osm_db() -> None:
    with connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS osm_collections (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                collected_at          TEXT    NOT NULL,
                parking_spots_count   INTEGER NOT NULL DEFAULT 0,
                parking_lots_count    INTEGER NOT NULL DEFAULT 0,
                ztl_zones_count       INTEGER NOT NULL DEFAULT 0,
                pedestrian_areas_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS parking_spots (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                osm_id   INTEGER NOT NULL,
                osm_type TEXT    NOT NULL,
                lat      REAL    NOT NULL,
                lon      REAL    NOT NULL,
                nome     TEXT,
                via      TEXT,
                posti    INTEGER,
                tariffa  TEXT,
                orari    TEXT,
                tipo     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pspot_osm ON parking_spots(osm_id);

            CREATE TABLE IF NOT EXISTS parking_lots (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                osm_id   INTEGER NOT NULL,
                osm_type TEXT    NOT NULL,
                lat      REAL    NOT NULL,
                lon      REAL    NOT NULL,
                nome     TEXT,
                posti    INTEGER,
                tipo     TEXT,
                accesso  TEXT,
                tariffa  TEXT,
                orari    TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_plot_osm ON parking_lots(osm_id);

            CREATE TABLE IF NOT EXISTS ztl_zones (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                osm_id           INTEGER NOT NULL,
                osm_type         TEXT    NOT NULL,
                nome             TEXT,
                orari            TEXT,
                accesso          TEXT,
                coordinate_json  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ztl_osm ON ztl_zones(osm_id);

            CREATE TABLE IF NOT EXISTS pedestrian_areas (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                osm_id           INTEGER NOT NULL,
                osm_type         TEXT    NOT NULL,
                nome             TEXT,
                coordinate_json  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ped_osm ON pedestrian_areas(osm_id)
        """)


# ---------------------------------------------------------------------------
# Helpers parser
# ---------------------------------------------------------------------------

def _center(el: dict) -> tuple[float, float] | None:
    """Ritorna (lat, lon) del centro dell'elemento OSM."""
    if el["type"] == "node":
        lat, lon = el.get("lat"), el.get("lon")
    else:
        c = el.get("center", {})
        lat, lon = c.get("lat"), c.get("lon")
        if not lat:
            geom = el.get("geometry", [])
            if geom:
                lat, lon = geom[0]["lat"], geom[0]["lon"]
    if lat and lon:
        return float(lat), float(lon)
    return None


def _geom_json(el: dict) -> str | None:
    """Ritorna la geometria come stringa JSON [[lon,lat], ...] per ways."""
    geom = el.get("geometry", [])
    if geom:
        return json.dumps([[pt["lon"], pt["lat"]] for pt in geom])
    # Relations: usa i membri way (semplificato al centroide)
    return None


def _cap(tags: dict) -> int | None:
    raw = tags.get("capacity")
    if raw and str(raw).isdigit():
        return int(raw)
    return None


# ---------------------------------------------------------------------------
# Overpass fetch
# ---------------------------------------------------------------------------

async def _overpass(client: httpx.AsyncClient, query: str, label: str) -> list[dict]:
    try:
        resp = await client.post(
            OVERPASS_URL,
            data={"data": query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
        log.info("Overpass [%s]: %d elementi", label, len(elements))
        return elements
    except Exception as exc:
        log.warning("Overpass [%s] fallita: %s", label, exc)
        return []


# ---------------------------------------------------------------------------
# Raccolta principale
# ---------------------------------------------------------------------------

async def collect_osm_data(force: bool = False) -> dict:
    """
    Esegue la raccolta OSM e salva nel DB.
    Se i dati esistono già e force=False, ritorna i conteggi esistenti senza rifare.
    """
    # Controllo idempotenza
    with connect() as conn:
        existing = conn.execute(
            "SELECT parking_spots_count, parking_lots_count, "
            "ztl_zones_count, pedestrian_areas_count, collected_at "
            "FROM osm_collections ORDER BY id DESC LIMIT 1"
        ).fetchone()

    if existing and not force:
        log.info(
            "Dati OSM già presenti (raccolti il %s) — skip. "
            "Usa force=True per ri-raccogliere.",
            existing[4],
        )
        return {
            "already_collected": True,
            "collected_at": existing[4],
            "parking_spots": existing[0],
            "parking_lots": existing[1],
            "ztl_zones": existing[2],
            "pedestrian_areas": existing[3],
        }

    log.info("Avvio raccolta OSM Bologna (Overpass API)…")
    async with httpx.AsyncClient(timeout=120.0) as client:
        spots_els    = await _overpass(client, _Q_PARKING_SPOTS, "parking_spots")
        lots_els     = await _overpass(client, _Q_PARKING_LOTS,  "parking_lots")
        ztl_els      = await _overpass(client, _Q_ZTL,           "ztl_zones")
        ped_els      = await _overpass(client, _Q_PEDESTRIAN,     "pedestrian_areas")

    n_spots = _save_parking_spots(spots_els)
    n_lots  = _save_parking_lots(lots_els)
    n_ztl   = _save_ztl_zones(ztl_els)
    n_ped   = _save_pedestrian_areas(ped_els)

    now = datetime.now(timezone.utc).isoformat()
    with connect() as conn:
        conn.execute(
            """INSERT INTO osm_collections
               (collected_at, parking_spots_count, parking_lots_count,
                ztl_zones_count, pedestrian_areas_count)
               VALUES (?, ?, ?, ?, ?)""",
            (now, n_spots, n_lots, n_ztl, n_ped),
        )

    log.info(
        "Raccolta OSM completata: %d strisce, %d strutture, %d ZTL, %d pedonali",
        n_spots, n_lots, n_ztl, n_ped,
    )
    return {
        "already_collected": False,
        "collected_at": now,
        "parking_spots": n_spots,
        "parking_lots": n_lots,
        "ztl_zones": n_ztl,
        "pedestrian_areas": n_ped,
    }


# ---------------------------------------------------------------------------
# Salvataggio per categoria
# ---------------------------------------------------------------------------

def _save_parking_spots(elements: list[dict]) -> int:
    seen: set[int] = set()
    rows = []
    for el in elements:
        osm_id = el.get("id")
        if osm_id in seen:
            continue
        seen.add(osm_id)
        coords = _center(el)
        if not coords:
            continue
        lat, lon = coords
        tags = el.get("tags", {})
        rows.append((
            osm_id, el["type"], lat, lon,
            tags.get("name"),
            tags.get("addr:street") or tags.get("addr:full"),
            _cap(tags),
            tags.get("fee:conditional") or ("a pagamento" if tags.get("fee") == "yes" else None),
            tags.get("opening_hours"),
            tags.get("parking") or tags.get("parking:lane:both"),
        ))
    if rows:
        with connect() as conn:
            conn.executemany(
                """INSERT INTO parking_spots
                   (osm_id, osm_type, lat, lon, nome, via, posti, tariffa, orari, tipo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
    return len(rows)


def _save_parking_lots(elements: list[dict]) -> int:
    seen: set[int] = set()
    rows = []
    for el in elements:
        osm_id = el.get("id")
        if osm_id in seen:
            continue
        seen.add(osm_id)
        coords = _center(el)
        if not coords:
            continue
        lat, lon = coords
        tags = el.get("tags", {})
        rows.append((
            osm_id, el["type"], lat, lon,
            tags.get("name") or tags.get("operator"),
            _cap(tags),
            tags.get("parking"),
            tags.get("access"),
            tags.get("fee:conditional") or ("a pagamento" if tags.get("fee") == "yes" else None),
            tags.get("opening_hours"),
        ))
    if rows:
        with connect() as conn:
            conn.executemany(
                """INSERT INTO parking_lots
                   (osm_id, osm_type, lat, lon, nome, posti, tipo, accesso, tariffa, orari)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
    return len(rows)


def _save_ztl_zones(elements: list[dict]) -> int:
    seen: set[int] = set()
    rows = []
    for el in elements:
        osm_id = el.get("id")
        if osm_id in seen:
            continue
        seen.add(osm_id)
        tags = el.get("tags", {})
        geom = _geom_json(el)
        rows.append((
            osm_id, el["type"],
            tags.get("name") or tags.get("description"),
            tags.get("opening_hours") or tags.get("access:conditional"),
            tags.get("access"),
            geom,
        ))
    if rows:
        with connect() as conn:
            conn.executemany(
                """INSERT INTO ztl_zones
                   (osm_id, osm_type, nome, orari, accesso, coordinate_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                rows,
            )
    return len(rows)


def _save_pedestrian_areas(elements: list[dict]) -> int:
    seen: set[int] = set()
    rows = []
    for el in elements:
        osm_id = el.get("id")
        if osm_id in seen:
            continue
        seen.add(osm_id)
        tags = el.get("tags", {})
        geom = _geom_json(el)
        rows.append((
            osm_id, el["type"],
            tags.get("name"),
            geom,
        ))
    if rows:
        with connect() as conn:
            conn.executemany(
                """INSERT INTO pedestrian_areas
                   (osm_id, osm_type, nome, coordinate_json)
                   VALUES (?, ?, ?, ?)""",
                rows,
            )
    return len(rows)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_osm_stats() -> dict:
    """Conta i record per categoria e ritorna l'ultima raccolta."""
    with connect() as conn:
        last = conn.execute(
            "SELECT collected_at, parking_spots_count, parking_lots_count, "
            "ztl_zones_count, pedestrian_areas_count "
            "FROM osm_collections ORDER BY id DESC LIMIT 1"
        ).fetchone()

        counts = {
            "parking_spots":    conn.execute("SELECT COUNT(*) FROM parking_spots").fetchone()[0],
            "parking_lots":     conn.execute("SELECT COUNT(*) FROM parking_lots").fetchone()[0],
            "ztl_zones":        conn.execute("SELECT COUNT(*) FROM ztl_zones").fetchone()[0],
            "pedestrian_areas": conn.execute("SELECT COUNT(*) FROM pedestrian_areas").fetchone()[0],
        }

    return {
        "last_collection": {
            "collected_at":           last[0] if last else None,
            "parking_spots_count":    last[1] if last else 0,
            "parking_lots_count":     last[2] if last else 0,
            "ztl_zones_count":        last[3] if last else 0,
            "pedestrian_areas_count": last[4] if last else 0,
        } if last else None,
        "current_db_counts": counts,
        "total": sum(counts.values()),
    }
