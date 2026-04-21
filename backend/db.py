"""
Database adapter: PostgreSQL se DATABASE_URL è impostata, altrimenti SQLite.

Uso:
    from db import connect

    with connect() as conn:
        conn.execute("SELECT ...")
        conn.executemany("INSERT ...", rows)
        conn.executescript("CREATE TABLE ...")
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_IS_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))

DB_PATH = Path(__file__).parent / "parking_history.db"


# ---------------------------------------------------------------------------
# Wrapper PostgreSQL — espone la stessa interfaccia di sqlite3.Connection
# ---------------------------------------------------------------------------

class _Cursor:
    """Wrap psycopg2 cursor per compatibilità con sqlite3."""
    def __init__(self, cur):
        self._c = cur

    def fetchone(self):
        return self._c.fetchone()

    def fetchall(self):
        return self._c.fetchall()


class _Conn:
    """Wrap psycopg2 connection con l'interfaccia usata nel progetto."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=()):
        cur = self._conn.cursor()
        cur.execute(_adapt_sql(sql), params or ())
        return _Cursor(cur)

    def executemany(self, sql: str, rows):
        cur = self._conn.cursor()
        cur.executemany(_adapt_sql(sql), rows)

    def executescript(self, script: str):
        """Esegue più statement DDL separati da ';'."""
        cur = self._conn.cursor()
        for stmt in _split(script):
            cur.execute(_adapt_ddl(stmt))
        self._conn.commit()


# ---------------------------------------------------------------------------
# Helpers SQL
# ---------------------------------------------------------------------------

def _adapt_sql(sql: str) -> str:
    """Converte placeholder SQLite ? → psycopg2 %s."""
    return sql.replace("?", "%s")


def _adapt_ddl(sql: str) -> str:
    """Converte DDL SQLite → PostgreSQL."""
    sql = _adapt_sql(sql)
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    return sql


def _split(script: str) -> list[str]:
    return [s.strip() for s in script.split(";") if s.strip()]


# ---------------------------------------------------------------------------
# Context manager principale
# ---------------------------------------------------------------------------

@contextmanager
def connect():
    """
    Restituisce un oggetto connection compatibile con sqlite3.
    - PostgreSQL: usa DATABASE_URL, commit/rollback automatici.
    - SQLite:     usa parking_history.db locale.
    """
    if _IS_PG:
        import psycopg2
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        raw = psycopg2.connect(url)
        try:
            yield _Conn(raw)
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.close()
    else:
        with sqlite3.connect(DB_PATH) as raw:
            yield raw
