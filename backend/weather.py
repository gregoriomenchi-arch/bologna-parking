"""
Modulo meteo — OpenWeatherMap Current Weather API.

Quota gratuita: 1000 chiamate/giorno → cache 10 minuti (max ~144 chiamate/giorno).
Chiave in variabile d'ambiente OPENWEATHER_KEY.
"""

import os
import time
import logging
from dataclasses import dataclass

import httpx

log = logging.getLogger("bologna_parking.weather")

OPENWEATHER_KEY: str = os.environ.get("OPENWEATHER_KEY", "")
_API_URL = "https://api.openweathermap.org/data/2.5/weather"
_PARAMS_BASE = {"q": "Bologna,IT", "units": "metric", "lang": "it"}

_CACHE_TTL = 600.0          # 10 minuti
_cache_ts: float = 0.0
_cache_raw: dict | None = None


@dataclass
class MeteoAttuale:
    pioggia: bool
    pioggia_mm: float           # precipitazione ultima ora (0 se nessuna)
    temperatura: float          # °C
    vento_kmh: float
    descrizione: str            # es. "pioggia leggera"
    icona: str                  # codice icona OWM (es. "10d")


async def get_meteo() -> MeteoAttuale | None:
    """
    Ritorna le condizioni meteo attuali per Bologna.
    Cache 10 min. Ritorna None se la chiave API non è configurata o la chiamata fallisce.
    """
    global _cache_ts, _cache_raw

    if not OPENWEATHER_KEY:
        return None

    now = time.monotonic()
    if _cache_raw and (now - _cache_ts) < _CACHE_TTL:
        return _parse(_cache_raw)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                _API_URL,
                params={**_PARAMS_BASE, "appid": OPENWEATHER_KEY},
            )
            resp.raise_for_status()
            data = resp.json()

        _cache_raw = data
        _cache_ts = now
        meteo = _parse(data)
        log.info(
            "Meteo aggiornato: %s, %.1f°C, vento %.1f km/h, pioggia=%s",
            meteo.descrizione, meteo.temperatura, meteo.vento_kmh, meteo.pioggia,
        )
        return meteo

    except httpx.HTTPStatusError as exc:
        log.warning("OpenWeatherMap HTTP %d: %s", exc.response.status_code, exc)
    except httpx.TimeoutException:
        log.warning("OpenWeatherMap: timeout")
    except Exception as exc:
        log.warning("OpenWeatherMap: errore inatteso — %s", exc)

    # Fallback: restituisce l'ultimo dato in cache anche se scaduto
    if _cache_raw:
        log.info("OpenWeatherMap: usando cache scaduta come fallback")
        return _parse(_cache_raw)
    return None


def _parse(data: dict) -> MeteoAttuale:
    rain = data.get("rain", {})
    pioggia_mm = float(rain.get("1h", 0.0) or rain.get("3h", 0.0))
    weather_list = data.get("weather", [{}])
    weather = weather_list[0] if weather_list else {}
    main_cond = weather.get("main", "")
    wind_ms = float(data.get("wind", {}).get("speed", 0.0))

    return MeteoAttuale(
        pioggia=pioggia_mm > 0.0 or main_cond in ("Rain", "Drizzle", "Thunderstorm"),
        pioggia_mm=pioggia_mm,
        temperatura=float(data["main"]["temp"]),
        vento_kmh=round(wind_ms * 3.6, 1),
        descrizione=weather.get("description", ""),
        icona=weather.get("icon", ""),
    )


def meteo_to_dict(m: MeteoAttuale | None) -> dict:
    if m is None:
        return {"disponibile": False}
    return {
        "disponibile": True,
        "pioggia": m.pioggia,
        "pioggia_mm": m.pioggia_mm,
        "temperatura": m.temperatura,
        "vento_kmh": m.vento_kmh,
        "descrizione": m.descrizione,
        "icona": m.icona,
    }
