"""
Modulo UniBo — calendario accademico AA 2025/2026 e zone ad alto impatto.

Le lezioni e gli esami aumentano la domanda di parcheggio nelle zone universitarie.
Il calendario è hardcoded; da aggiornare ogni anno accademico.
"""

from datetime import date
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Calendario AA 2025/2026
# ---------------------------------------------------------------------------

_PERIODI_LEZIONI = [
    (date(2025, 9, 22), date(2025, 12, 20)),   # I semestre
    (date(2026, 2,  9), date(2026, 5, 30)),    # II semestre
]

_PERIODI_ESAMI = [
    (date(2026, 1,  7), date(2026, 2,  7)),    # Sessione invernale
    (date(2026, 6,  9), date(2026, 7, 25)),    # Sessione estiva
    (date(2026, 9,  1), date(2026, 9, 15)),    # Sessione autunnale
]

_PAUSA_ESTIVA = (date(2026, 7, 26), date(2026, 9, 8))


# ---------------------------------------------------------------------------
# Zone universitarie — (nome, lat_centro, lon_centro, raggio_km)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ZonaUnibo:
    nome: str
    lat: float
    lon: float
    raggio_km: float


ZONE_UNIBO: list[ZonaUnibo] = [
    ZonaUnibo("Zamboni/Irnerio",  44.4975, 11.3530, 0.55),  # Lettere, Scienze Politiche, Giurisprudenza
    ZonaUnibo("Belmeloro",        44.4956, 11.3680, 0.40),  # Economia
    ZonaUnibo("Sant'Orsola",      44.4908, 11.3700, 0.60),  # Medicina, Farmacia, Odontoiatria
]


# ---------------------------------------------------------------------------
# Funzioni pubbliche
# ---------------------------------------------------------------------------

def _in_periodo(d: date, periodi: list[tuple[date, date]]) -> bool:
    return any(inizio <= d <= fine for inizio, fine in periodi)


def is_giorno_lezioni(d: date | None = None) -> bool:
    """True se oggi (o la data fornita) è un giorno di lezione UniBo (lu-ve, non festivo)."""
    d = d or date.today()
    if d.weekday() >= 5:        # sabato/domenica
        return False
    return _in_periodo(d, _PERIODI_LEZIONI)


def is_sessione_esami(d: date | None = None) -> bool:
    """True se oggi (o la data fornita) è in sessione esami (lu-ve)."""
    d = d or date.today()
    if d.weekday() >= 5:
        return False
    return _in_periodo(d, _PERIODI_ESAMI)


def is_pausa_estiva(d: date | None = None) -> bool:
    d = d or date.today()
    return _PAUSA_ESTIVA[0] <= d <= _PAUSA_ESTIVA[1]


def is_zona_universitaria(lat: float, lon: float) -> bool:
    """True se le coordinate ricadono in almeno una zona universitaria."""
    import math
    for zona in ZONE_UNIBO:
        dlat = math.radians(lat - zona.lat)
        dlon = math.radians(lon - zona.lon)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(zona.lat)) * math.cos(math.radians(lat))
             * math.sin(dlon / 2) ** 2)
        dist = 6371.0 * 2 * math.asin(math.sqrt(a))
        if dist <= zona.raggio_km:
            return True
    return False


def get_status(d: date | None = None) -> dict:
    """Stato calendario UniBo per la data fornita (default: oggi)."""
    d = d or date.today()
    return {
        "data": d.isoformat(),
        "giorno_settimana": d.weekday(),        # 0=lun, 6=dom
        "giorno_lezioni": is_giorno_lezioni(d),
        "sessione_esami": is_sessione_esami(d),
        "pausa_estiva": is_pausa_estiva(d),
        "zone": [{"nome": z.nome, "lat": z.lat, "lon": z.lon, "raggio_km": z.raggio_km}
                 for z in ZONE_UNIBO],
    }
