"""
Modulo ZTL Sirio — Bologna.

Orari ZTL Sirio (centro storico):
  - Lunedì–Venerdì: 07:00–20:00
  - Sabato:         14:00–20:00
  - Domenica:       chiusa
  - Festivi nazionali: chiusa

Buffer effect: quando la ZTL sta per attivarsi (< 30 min),
le strade appena fuori dalla ZTL (buffer 0.65–1.2 km dal centro)
subiscono un aumento di domanda → penalità score.
"""

import math
from datetime import date, datetime, timedelta, timezone

# Centro geometrico approssimativo della ZTL Sirio (Piazza Maggiore)
_CENTER_LAT = 44.4938
_CENTER_LON = 11.3427

_ZTL_INNER_KM = 0.65    # strade dentro la ZTL (accesso limitato)
_ZTL_BUFFER_KM = 1.20   # strade nel buffer esterno (risentono dell'effetto ZTL)

# Festivi nazionali italiani (giorno, mese)
_FESTIVI = {
    (1, 1), (6, 1), (25, 4), (1, 5), (2, 6),
    (15, 8), (1, 11), (8, 12), (25, 12), (26, 12),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _is_festivo(d: date) -> bool:
    return (d.day, d.month) in _FESTIVI


def _local_now() -> datetime:
    """Ora corrente nel fuso italiano (CET/CEST) senza pytz."""
    utc = datetime.now(timezone.utc)
    # CEST (UTC+2): ultima domenica marzo – ultima domenica ottobre
    year = utc.year
    # Calcola ultima domenica di marzo
    last_sun_mar = max(
        date(year, 3, d)
        for d in range(25, 32)
        if date(year, 3, d).weekday() == 6
    )
    # Calcola ultima domenica di ottobre
    last_sun_oct = max(
        date(year, 10, d)
        for d in range(25, 32)
        if date(year, 10, d).weekday() == 6
    )
    offset_h = 2 if last_sun_mar <= utc.date() < last_sun_oct else 1
    return (utc + timedelta(hours=offset_h)).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Funzioni pubbliche
# ---------------------------------------------------------------------------

def is_ztl_attiva(dt: datetime | None = None) -> bool:
    """True se la ZTL Sirio è attiva al momento dt (default: ora corrente italiana)."""
    dt = dt or _local_now()
    d = dt.date() if hasattr(dt, "date") else dt
    if _is_festivo(d) or d.weekday() == 6:   # festivo o domenica
        return False
    h = dt.hour + dt.minute / 60.0
    if d.weekday() == 5:                      # sabato
        return 14.0 <= h < 20.0
    return 7.0 <= h < 20.0                    # lun–ven


def prossima_attivazione(dt: datetime | None = None) -> datetime | None:
    """
    Ritorna il datetime (ora italiana locale) della prossima attivazione ZTL.
    Ritorna None se la ZTL è già attiva ora.
    Cerca entro 7 giorni.
    """
    dt = dt or _local_now()
    if is_ztl_attiva(dt):
        return None

    # Controlla i prossimi 7 giorni cercando il primo orario di attivazione
    for day_delta in range(8):
        d = dt.date() + timedelta(days=day_delta)
        if _is_festivo(d) or d.weekday() == 6:
            continue
        start_hour = 14 if d.weekday() == 5 else 7
        candidate = datetime(d.year, d.month, d.day, start_hour, 0, 0)
        if candidate > dt:
            return candidate
    return None


def minuti_a_attivazione(dt: datetime | None = None) -> int | None:
    """Minuti alla prossima attivazione ZTL. None se già attiva."""
    dt = dt or _local_now()
    nxt = prossima_attivazione(dt)
    if nxt is None:
        return None
    return max(0, int((nxt - dt).total_seconds() / 60))


def prossima_disattivazione(dt: datetime | None = None) -> datetime | None:
    """
    Ritorna il datetime della prossima disattivazione ZTL (fine periodo attivo).
    Ritorna None se ZTL non è attiva.
    """
    dt = dt or _local_now()
    if not is_ztl_attiva(dt):
        return None
    d = dt.date()
    end_hour = 20
    return datetime(d.year, d.month, d.day, end_hour, 0, 0)


def is_in_ztl(lat: float, lon: float) -> bool:
    """True se le coordinate sono all'interno della ZTL (< 0.65 km dal centro)."""
    return _haversine_km(lat, lon, _CENTER_LAT, _CENTER_LON) < _ZTL_INNER_KM


def is_nel_buffer_ztl(lat: float, lon: float) -> bool:
    """True se le coordinate sono nel buffer esterno ZTL (0.65–1.2 km dal centro)."""
    dist = _haversine_km(lat, lon, _CENTER_LAT, _CENTER_LON)
    return _ZTL_INNER_KM <= dist <= _ZTL_BUFFER_KM


def get_status(dt: datetime | None = None) -> dict:
    """Stato ZTL corrente con orario prossima attivazione/disattivazione."""
    dt = dt or _local_now()
    attiva = is_ztl_attiva(dt)
    minuti = minuti_a_attivazione(dt)
    disatt = prossima_disattivazione(dt)
    nxt_att = prossima_attivazione(dt)
    return {
        "attiva": attiva,
        "orario_locale": dt.strftime("%H:%M"),
        "giorno_settimana": dt.weekday(),        # 0=lun, 6=dom
        "minuti_a_attivazione": minuti,
        "prossima_attivazione": nxt_att.strftime("%H:%M") if nxt_att else None,
        "prossima_disattivazione": disatt.strftime("%H:%M") if disatt else None,
        "attiva_tra_30_min": (minuti is not None and minuti <= 30),
    }
