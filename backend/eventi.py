"""
Modulo eventi Bologna.

Fase attuale: eventi di test hardcoded, riseminati ad ogni avvio.
Fase futura:  refresh_eventi() chiamerà gli scraper reali (bolognfc.com, ecc.).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from pydantic import BaseModel

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
    with connect() as conn:
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
    now_s  = now.isoformat()
    cut_s  = cutoff.isoformat()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, nome, venue, data_inizio, data_fine,
                   lat, lon, impatto, raggio_km, fonte
            FROM eventi
            WHERE
              (data_inizio <= ? AND (data_fine IS NULL OR data_fine >= ?))
              OR (data_inizio > ? AND data_inizio <= ?)
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

    with connect() as conn:
        existing_names = {
            r[0] for r in conn.execute("SELECT nome FROM eventi").fetchall()
        }
    test_events = [e for e in [
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
    ] if e.nome not in existing_names]

    _save_events(test_events)
    log.info("Seed eventi di test: %d nuovi eventi inseriti", len(test_events))


# ---------------------------------------------------------------------------
# Scraper reale — Bologna FC (football-data.org, free tier)
# ---------------------------------------------------------------------------

async def _fetch_bologna_fc() -> list[Evento]:
    """
    Scarica le prossime partite del Bologna FC da football-data.org.
    Team ID Bologna FC = 98, competizione Serie A = SA.
    """
    url = "https://api.football-data.org/v4/teams/98/matches"
    params = {"status": "SCHEDULED,LIVE", "limit": 10}
    venue = VENUES["Stadio Renato Dall'Ara"]
    eventi = []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params,
                                    headers={"X-Auth-Token": ""})
            if not resp.ok:
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
            if match.get("homeTeam", {}).get("id") != 98:
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
# Scraper reale — Fiera di Bologna (HTML scraping)
# ---------------------------------------------------------------------------

async def _fetch_fiera_bologna() -> list[Evento]:
    """
    Scarica le manifestazioni da Fiera Bologna via HTML pubblico.
    """
    import re
    url = "https://www.bolognafiere.it/it/manifestazioni.html"
    venue = VENUES["Fiera di Bologna"]
    eventi = []
    try:
        async with httpx.AsyncClient(timeout=10.0,
                                     follow_redirects=True) as client:
            resp = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 Bologna Parking App"
            })
            if not resp.ok:
                return []
            html = resp.text

        pattern = re.compile(
            r'<h\d[^>]*>\s*([^<]{5,80})\s*</h\d>.*?'
            r'(\d{2}/\d{2}/\d{4})\s*[–\-]\s*(\d{2}/\d{2}/\d{4})',
            re.DOTALL
        )
        now = datetime.now(timezone.utc)
        for m in pattern.finditer(html):
            nome = m.group(1).strip()[:80]
            try:
                dt_start = datetime.strptime(
                    m.group(2), "%d/%m/%Y").replace(tzinfo=timezone.utc)
                dt_fine = datetime.strptime(
                    m.group(3), "%d/%m/%Y").replace(
                    hour=22, tzinfo=timezone.utc)
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
    except Exception as exc:
        log.warning("Fiera Bologna scraping error: %s", exc)
    return eventi


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
