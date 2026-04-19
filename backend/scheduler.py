"""
Scheduler notturno — raccoglie dati SostaBo ogni 10 minuti.

Eseguire con:
    python scheduler.py

Gira in background tutta la notte per accumulare storico reale.
Log su stdout (rediretto a scheduler.log da start_background.bat).
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone

# Aggiungi la directory del file al path per importare i moduli locali
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from sostabo import SostaBoClient
from historical_data import init_db, save_readings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scheduler")

INTERVAL_SEC = 600  # 10 minuti


async def collect_once() -> int:
    """Raccoglie una volta e salva. Ritorna il numero di record salvati."""
    async with SostaBoClient() as client:
        parcheggi = await client.get_disponibilita()
    save_readings(parcheggi)
    return len(parcheggi)


async def main() -> None:
    init_db()
    log.info("Scheduler avviato — raccolta ogni %d secondi (%.0f min)", INTERVAL_SEC, INTERVAL_SEC / 60)

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        try:
            n = await collect_once()
            log.info("[ciclo %d] %s — salvati %d record SostaBo", cycle, ts, n)
        except Exception as exc:
            log.warning("[ciclo %d] %s — raccolta fallita: %s", cycle, ts, exc)

        await asyncio.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Scheduler fermato dall'utente.")
