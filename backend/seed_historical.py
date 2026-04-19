"""
Seed del database storico con dati simulati realistici per Bologna.
Esegui una volta: python seed_historical.py
"""
import sqlite3
import math
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent / "parking_history.db"

# Scambiatori di Bologna con coordinate reali e profilo occupazione
# occ_peak = % occupazione ore di punta (8-19 feriali)
# occ_off  = % occupazione ore serali/notturne e fine settimana
SCAMBIATORI = [
    # nome, lat, lon, occ_peak, occ_off
    # --- Centro / ZTL (difficilissimo) ---
    ("Parcheggio Malpighi",        44.4960, 11.3390, 88, 45),
    ("Parcheggio Via A. Saffi",    44.4945, 11.3310, 85, 40),
    # --- Stazione (difficilissimo) ---
    ("Parcheggio Pietramellara",   44.5054, 11.3416, 90, 55),
    # --- Zona universitaria/ospedale (difficile) ---
    ("Parcheggio Sant'Orsola",     44.4890, 11.3630, 80, 35),
    ("Parcheggio Staveco",         44.4790, 11.3650, 75, 30),
    # --- Via Saragozza / Via Saffi (medio-difficile) ---
    ("Parcheggio Saragozza",       44.4920, 11.3250, 70, 30),
    # --- Via Murri / Mazzini (difficile) ---
    ("Parcheggio Murri",           44.4870, 11.3560, 75, 32),
    # --- Viali periferici (più facile) ---
    ("Parcheggio Prati di Caprara",44.5050, 11.3180, 42, 18),
    ("Parcheggio Costa",           44.4945, 11.3139, 45, 20),
    # --- Fiera / Stalingrado (facile) ---
    ("Parcheggio Fiera",           44.5000, 11.3650, 38, 15),
    # --- Periferia est (facile) ---
    ("Parcheggio Savena",          44.4660, 11.3900, 28, 12),
    ("Parcheggio Emilia Est",      44.4828, 11.3765, 32, 14),
]


def _occ_for_hour(occ_peak: float, occ_off: float, ora: int, giorno: int) -> float:
    """Calcola occupazione % realistica per ora e giorno della settimana."""
    is_weekend = giorno >= 5  # sabato=5, domenica=6

    if is_weekend:
        # Sabato/domenica: picco 10-13 e 15-19, poi cala
        if 10 <= ora <= 13 or 15 <= ora <= 19:
            base = occ_peak * 0.65
        elif 20 <= ora <= 22:
            base = occ_peak * 0.45
        else:
            base = occ_off * 0.5
    else:
        # Feriali: picco 8-12 e 14-18
        if 8 <= ora <= 12:
            base = occ_peak * (0.8 + 0.2 * (ora - 8) / 4)  # sale fino a 10
        elif 13 == ora:
            base = occ_peak * 0.7  # pranzo leggero calo
        elif 14 <= ora <= 18:
            base = occ_peak
        elif 19 <= ora <= 21:
            base = occ_peak * 0.55
        elif 22 <= ora or ora <= 5:
            base = occ_off * 0.4
        else:  # 6-7
            base = occ_off * 0.6

    # Aggiungi leggera variazione casuale (deterministica via hash)
    seed = int(occ_peak * 100 + ora * 7 + giorno * 31)
    noise = ((seed * 1103515245 + 12345) & 0x7FFFFFFF) % 11 - 5  # -5..+5
    return round(min(100.0, max(0.0, base + noise)), 1)


def seed():
    now = datetime.now(timezone.utc)
    rows = []
    # Generiamo campioni per ogni ora × ogni giorno della settimana
    # 3 campioni per slot per simulare storico più robusto
    for nome, lat, lon, occ_peak, occ_off in SCAMBIATORI:
        totale = 150  # posti totali medi
        for giorno in range(7):
            for ora in range(24):
                for campione in range(3):
                    occ_pct = _occ_for_hour(occ_peak, occ_off, ora, giorno)
                    # Piccola variazione tra campioni
                    occ_pct = round(min(100, max(0, occ_pct + (campione - 1) * 2)), 1)
                    occupati = round(totale * occ_pct / 100)
                    liberi = totale - occupati
                    rows.append((
                        now.isoformat(), ora, giorno,
                        nome, liberi, occupati, totale,
                        occ_pct, lat, lon,
                    ))

    with sqlite3.connect(DB_PATH) as conn:
        # Rimuovi solo i dati seed precedenti (preserva dati reali)
        conn.execute("DELETE FROM readings WHERE parcheggio_nome LIKE 'Parcheggio %'")
        conn.executemany(
            """INSERT INTO readings
               (timestamp, ora, giorno_settimana, parcheggio_nome,
                posti_liberi, posti_occupati, posti_totali, occupazione_pct, lat, lon)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
    print(f"Inseriti {len(rows)} record storici per {len(SCAMBIATORI)} scambiatori")


if __name__ == "__main__":
    seed()
