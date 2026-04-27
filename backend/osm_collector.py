"""
Raccolta una-tantum dati OSM per Bologna via Overpass API.

Categorie raccolte:
  - parking_spots   : strisce blu e parcheggi a pagamento su strada
  - parking_lots    : strutture (multi-piano, sotterranee, parcheggi di scambio)
  - ztl_zones       : zone a traffico limitato con poligoni e orari
  - pedestrian_areas: aree pedonali (impediscono transito veicolare)

Idempotente: non ri-esegue se parking_spots ha già righe con dati reali.
Fallback: se Overpass restituisce 0 strisce blu, inserisce dati statici hardcoded.
"""

import json
import logging
import traceback
from datetime import datetime, timezone

import httpx

from db import connect

log = logging.getLogger("bologna_parking.osm")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BBOX = "44.44,11.27,44.56,11.44"       # intero comune di Bologna

# ---------------------------------------------------------------------------
# Query Overpass — timeout 60s per query
# ---------------------------------------------------------------------------

_Q_PARKING_SPOTS = f"""[out:json][timeout:60];
(
  node["amenity"="parking"]["fee"="yes"]({BBOX});
  way["amenity"="parking"]["fee"="yes"]({BBOX});
  node["amenity"="parking"]["parking"="street_side"]({BBOX});
  way["amenity"="parking"]["parking"="street_side"]({BBOX});
  node["parking"="street_side"]["fee"="yes"]({BBOX});
  way["parking"="street_side"]["fee"="yes"]({BBOX});
  way["parking:lane:right"="parallel"]["parking:lane:right:fee"="yes"]({BBOX});
  way["parking:lane:left"="parallel"]["parking:lane:left:fee"="yes"]({BBOX});
  way["parking:lane:both"="parallel"]["parking:lane:both:fee"="yes"]({BBOX});
);
out center tags;"""

_Q_PARKING_LOTS = f"""[out:json][timeout:60];
(
  node["amenity"="parking"]["parking"~"multi-storey|underground"]({BBOX});
  way["amenity"="parking"]["parking"~"multi-storey|underground"]({BBOX});
  node["amenity"="parking"]["access"~"^(yes|customers)$"]({BBOX});
  way["amenity"="parking"]["access"~"^(yes|customers)$"]({BBOX});
);
out center tags;"""

_Q_ZTL = f"""[out:json][timeout:60];
(
  relation["boundary"="restricted_area"]({BBOX});
  way["boundary"="restricted_area"]({BBOX});
  way["motor_vehicle"="no"]["access"="no"]({BBOX});
  way["access"="no"]["highway"~"residential|tertiary|secondary"]({BBOX});
);
out geom tags;"""

_Q_PEDESTRIAN = f"""[out:json][timeout:60];
(
  way["highway"="pedestrian"]["area"="yes"]({BBOX});
  way["highway"="pedestrian"]["area"!="yes"]({BBOX});
  relation["highway"="pedestrian"]({BBOX});
  way["foot"="designated"]["motor_vehicle"="no"]["area"="yes"]({BBOX});
);
out geom tags;"""


# ---------------------------------------------------------------------------
# Dati statici di fallback — 25 strisce blu reali a Bologna
# ---------------------------------------------------------------------------
# Tuple: (nome, via, lat, lon, posti, tariffa, orari, tipo)
_STATIC_SPOTS: list[tuple] = [
    ("Strisce blu Via Zamboni",      "Via Zamboni",           44.4970, 11.3559, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Zamboni N",    "Via Zamboni",           44.4978, 11.3571, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Irnerio",      "Via Irnerio",           44.5001, 11.3492, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Irnerio O",    "Via Irnerio",           44.4997, 11.3478, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Saragozza",    "Via Saragozza",         44.4893, 11.3301, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Saragozza E",  "Via Saragozza",         44.4885, 11.3318, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Murri",        "Via Murri",             44.4843, 11.3622, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Murri S",      "Via Murri",             44.4851, 11.3638, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Mazzini",      "Via Giuseppe Mazzini",  44.4935, 11.3445, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Mazzini N",    "Via Giuseppe Mazzini",  44.4928, 11.3461, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Viale Fiera",      "Viale della Fiera",     44.5064, 11.3567, None, "a pagamento", "Lu-Do 7-22", "street_side"),
    ("Strisce blu Viale Michelino",  "Viale Michelino",       44.5072, 11.3583, None, "a pagamento", "Lu-Do 7-22", "street_side"),
    ("Strisce blu Via Amendola",     "Via Giovanni Amendola", 44.5049, 11.3430, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Amendola E",   "Via Giovanni Amendola", 44.5039, 11.3418, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Ugo Bassi",    "Via Ugo Bassi",         44.4939, 11.3428, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Rizzoli",      "Via Rizzoli",           44.4947, 11.3412, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Andrea Costa", "Via Andrea Costa",      44.4920, 11.3201, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via A. Costa N",   "Via Andrea Costa",      44.4908, 11.3215, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Massarenti",   "Via Massarenti",        44.4865, 11.3685, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Massarenti N", "Via Massarenti",        44.4871, 11.3671, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Lame",         "Via Lame",              44.5023, 11.3325, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Lame N",       "Via Lame",              44.5031, 11.3341, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Emilia Est",   "Via Emilia Est",        44.4897, 11.3652, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via Emilia Ovest", "Via Emilia Ovest",      44.4958, 11.3151, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
    ("Strisce blu Via San Donato",   "Via San Donato",        44.5038, 11.3612, None, "a pagamento", "Lu-Sa 8-20", "street_side"),
]

# OSM id fittizio negativo per distinguere i dati statici dai dati OSM reali
_STATIC_OSM_ID_START = -1000


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
            CREATE INDEX IF NOT EXISTS idx_ped_osm ON pedestrian_areas(osm_id);

            CREATE TABLE IF NOT EXISTS zone_sosta_cache (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nome_via    TEXT NOT NULL UNIQUE,
                aggiornato  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_zsosta_via ON zone_sosta_cache(nome_via)
        """)


# ---------------------------------------------------------------------------
# Helpers parser
# ---------------------------------------------------------------------------

def _center(el: dict) -> tuple[float, float] | None:
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
    geom = el.get("geometry", [])
    if geom:
        return json.dumps([[pt["lon"], pt["lat"]] for pt in geom])
    return None


def _cap(tags: dict) -> int | None:
    raw = tags.get("capacity")
    if raw and str(raw).isdigit():
        return int(raw)
    return None


# ---------------------------------------------------------------------------
# Overpass fetch — con logging dettagliato
# ---------------------------------------------------------------------------

async def _overpass(client: httpx.AsyncClient, query: str, label: str) -> list[dict]:
    log.info("Overpass [%s]: invio query (%d chars)…", label, len(query))
    try:
        resp = await client.post(
            OVERPASS_URL,
            data={"data": query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        log.info(
            "Overpass [%s]: HTTP %d, content-length=%s",
            label, resp.status_code, resp.headers.get("content-length", "?"),
        )

        if resp.status_code != 200:
            snippet = resp.text[:500] if resp.text else "(vuoto)"
            log.error("Overpass [%s]: risposta non-200 — body: %s", label, snippet)
            return []

        try:
            data = resp.json()
        except Exception as json_exc:
            snippet = resp.text[:300] if resp.text else "(vuoto)"
            log.error("Overpass [%s]: JSON non valido (%s) — body: %s", label, json_exc, snippet)
            return []

        elements = data.get("elements", [])
        log.info("Overpass [%s]: %d elementi ricevuti", label, len(elements))
        remark = data.get("remark", "")
        if remark:
            log.warning("Overpass [%s]: remark server — %s", label, remark)
        return elements

    except httpx.TimeoutException as exc:
        log.error("Overpass [%s]: TIMEOUT — %s", label, exc)
        return []
    except httpx.ConnectError as exc:
        log.error("Overpass [%s]: CONNESSIONE FALLITA — %s", label, exc)
        return []
    except Exception as exc:
        log.error("Overpass [%s]: errore inatteso — %s\n%s", label, exc, traceback.format_exc())
        return []


# ---------------------------------------------------------------------------
# Raccolta principale
# ---------------------------------------------------------------------------

async def collect_osm_data(force: bool = False) -> dict:
    """
    Esegue la raccolta OSM e salva nel DB.
    Skip solo se parking_spots ha già righe (e force=False).
    Se Overpass restituisce 0 strisce blu, inserisce dati statici di fallback.
    """
    with connect() as conn:
        existing_spots = conn.execute("SELECT COUNT(*) FROM parking_spots").fetchone()[0]
        last_coll = conn.execute(
            "SELECT collected_at FROM osm_collections ORDER BY id DESC LIMIT 1"
        ).fetchone()

    if existing_spots > 0 and not force:
        log.info(
            "Dati OSM già presenti (%d strisce, raccolti il %s) — skip. "
            "Usa force=True per ri-raccogliere.",
            existing_spots, last_coll[0] if last_coll else "?",
        )
        with connect() as conn:
            last = conn.execute(
                "SELECT parking_spots_count, parking_lots_count, "
                "ztl_zones_count, pedestrian_areas_count, collected_at "
                "FROM osm_collections ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return {
            "already_collected": True,
            "collected_at": last[4] if last else None,
            "parking_spots": last[0] if last else existing_spots,
            "parking_lots":  last[1] if last else 0,
            "ztl_zones":     last[2] if last else 0,
            "pedestrian_areas": last[3] if last else 0,
        }

    if force:
        log.info("Raccolta OSM forzata — pulizia tabelle esistenti…")
        with connect() as conn:
            conn.executescript("""
                DELETE FROM parking_spots;
                DELETE FROM parking_lots;
                DELETE FROM ztl_zones;
                DELETE FROM pedestrian_areas;
                DELETE FROM osm_collections
            """)

    log.info("Avvio raccolta OSM Bologna (Overpass API, timeout=70s per query)…")
    # 70s client timeout: 60s elaborazione server + 10s margine rete
    async with httpx.AsyncClient(timeout=70.0) as client:
        spots_els = await _overpass(client, _Q_PARKING_SPOTS, "parking_spots")
        lots_els  = await _overpass(client, _Q_PARKING_LOTS,  "parking_lots")
        ztl_els   = await _overpass(client, _Q_ZTL,           "ztl_zones")
        ped_els   = await _overpass(client, _Q_PEDESTRIAN,     "pedestrian_areas")

    n_spots = _save_parking_spots(spots_els)
    n_lots  = _save_parking_lots(lots_els)
    n_ztl   = _save_ztl_zones(ztl_els)
    n_ped   = _save_pedestrian_areas(ped_els)

    # Fallback: se Overpass non ha restituito strisce blu, usa dati statici
    if n_spots == 0:
        log.warning(
            "Overpass non ha restituito strisce blu — "
            "inserimento %d spot statici hardcoded per Bologna",
            len(_STATIC_SPOTS),
        )
        n_spots = _save_static_spots()

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
            tags.get("parking") or tags.get("parking:lane:right") or tags.get("parking:lane:both"),
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


def _save_static_spots() -> int:
    """Inserisce i 25 spot statici hardcoded di Bologna come fallback."""
    rows = [
        (
            _STATIC_OSM_ID_START - i,  # id negativo per distinguerli da OSM reali
            "static",
            lat, lon, nome, via, posti, tariffa, orari, tipo,
        )
        for i, (nome, via, lat, lon, posti, tariffa, orari, tipo) in enumerate(_STATIC_SPOTS)
    ]
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
# Zone sosta (strisce blu) — Open Data Comune di Bologna
# ---------------------------------------------------------------------------

async def fetch_zone_sosta() -> set[str]:
    """
    Scarica il dataset 'Zone sosta per via e civico' dal Comune di Bologna.
    Ritorna un set di nomi di via (lowercase) che hanno strisce blu.
    """
    base_url = (
        "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets"
        "/stradario-generale-al-25nov2022/records"
    )
    nomi: set[str] = set()
    offset = 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            resp = await client.get(base_url, params={"limit": 100, "offset": offset})
            if not resp.is_success:
                break
            data = resp.json()
            records = data.get("results", [])
            if not records:
                break
            for r in records:
                descr = r.get("descr") or ""
                # estrae "VIA STALINGRADO" da "STALINGRADO(VIA)" → "via stalingrado"
                if "(" in descr:
                    nome_raw, tipo_raw = descr.split("(", 1)
                    tipo = tipo_raw.rstrip(")").strip().lower()
                    nome = nome_raw.strip().lower()
                    via_completa = f"{tipo} {nome}".strip()
                else:
                    via_completa = descr.lower().strip()
                if via_completa:
                    nomi.add(via_completa)
            offset += 100
            if offset >= data.get("total_count", 0):
                break
    return nomi


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_osm_stats() -> dict:
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
