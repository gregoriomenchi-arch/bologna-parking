"""
Client per le API pubbliche dei parcheggi di Bologna.
Fonte: Comune di Bologna Open Data Portal (CC BY 4.0)
Aggiornamento dati: ogni ~10 minuti
"""

import httpx
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

BASE_URL = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets"

DISPONIBILITA_ENDPOINT = f"{BASE_URL}/disponibilita-parcheggi-vigente/records"
STRUTTURE_ENDPOINT     = f"{BASE_URL}/parcheggi/records"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_QUERY = """[out:json][timeout:30];
area["name"="Bologna"]["admin_level"="8"];
(
  node["amenity"="parking"](area);
  way["amenity"="parking"](area);
);
out center;"""

DEFAULT_TIMEOUT = 25.0   # secondi (Overpass può essere lento)


# ---------------------------------------------------------------------------
# Modelli dati
# ---------------------------------------------------------------------------

class Coordinate(BaseModel):
    lat: float
    lon: float


class ParcheggioDisponibilita(BaseModel):
    """Disponibilità in tempo reale di un parcheggio."""
    nome: str
    posti_liberi: int
    posti_occupati: int
    posti_totali: int
    occupazione_pct: float          # 0–100
    coordinate: Optional[Coordinate]
    aggiornato_at: Optional[datetime]


class ParcheggioStruttura(BaseModel):
    """Informazioni statiche su una struttura di sosta."""
    nome: str
    tipologia: Optional[str]        # "struttura" | "raso"
    posti_totali: Optional[int]
    tariffa: Optional[str]
    zona: Optional[str]
    coordinate: Optional[Coordinate]


class ParcheggioZona(BaseModel):
    """Parcheggio a pagamento nei quartieri (strisce blu/bianche, strutture)."""
    nome: str
    tipo: Optional[str]             # "strisce_blu" | "strisce_bianche" | "struttura" | "raso" | altro
    tariffa: Optional[str]
    zona: Optional[str]
    posti_totali: Optional[int]
    coordinate: Optional[Coordinate]


# ---------------------------------------------------------------------------
# Parser interni
# ---------------------------------------------------------------------------

def _parse_disponibilita(record: dict) -> ParcheggioDisponibilita:
    geo = record.get("coordinate") or record.get("geo_point_2d")
    coordinate = None
    if isinstance(geo, dict) and "lat" in geo and "lon" in geo:
        coordinate = Coordinate(lat=geo["lat"], lon=geo["lon"])

    raw_ts = record.get("data")
    aggiornato_at = None
    if raw_ts:
        try:
            aggiornato_at = datetime.fromisoformat(raw_ts)
        except ValueError:
            pass

    return ParcheggioDisponibilita(
        nome=record.get("parcheggio", "N/D"),
        posti_liberi=int(record.get("posti_liberi") or 0),
        posti_occupati=int(record.get("posti_occupati") or 0),
        posti_totali=int(record.get("posti_totali") or 0),
        occupazione_pct=float(record.get("occupazione") or 0.0),
        coordinate=coordinate,
        aggiornato_at=aggiornato_at,
    )


# OSM parking=* → tipo interno
_OSM_PARKING_TIPO = {
    "multi-storey":  "struttura",
    "underground":   "struttura",
    "rooftop":       "struttura",
    "surface":       "raso",
    "lane":          "strisce_bianche",
    "street_side":   "strisce_bianche",
    "sheds":         "raso",
    "carports":      "raso",
    "garage_boxes":  "struttura",
}


def _parse_zona_osm(element: dict) -> Optional["ParcheggioZona"]:
    """Converte un elemento Overpass (node o way) in ParcheggioZona."""
    # Coordinate: node → lat/lon diretti; way → center
    if element["type"] == "node":
        lat, lon = element.get("lat"), element.get("lon")
    else:
        center = element.get("center", {})
        lat, lon = center.get("lat"), center.get("lon")

    if lat is None or lon is None:
        return None

    tags = element.get("tags", {})
    parking_val = tags.get("parking", "")
    tipo = _OSM_PARKING_TIPO.get(parking_val)

    # Se ha tariffa (fee=yes) e non è già struttura, classifica come strisce_blu
    if tipo == "raso" and tags.get("fee") == "yes":
        tipo = "strisce_blu"
    elif tipo is None and tags.get("fee") == "yes":
        tipo = "strisce_blu"

    nome = tags.get("name") or tags.get("operator") or f"Parcheggio OSM {element['id']}"
    capacita = tags.get("capacity")

    return ParcheggioZona(
        nome=nome,
        tipo=tipo,
        tariffa=tags.get("fee:conditional") or ("a pagamento" if tags.get("fee") == "yes" else None),
        zona=tags.get("addr:suburb") or tags.get("addr:quarter"),
        posti_totali=int(capacita) if capacita and capacita.isdigit() else None,
        coordinate=Coordinate(lat=lat, lon=lon),
    )


def _parse_struttura(record: dict) -> ParcheggioStruttura:
    geo = record.get("geo_point_2d")
    coordinate = None
    if isinstance(geo, dict) and "lat" in geo and "lon" in geo:
        coordinate = Coordinate(lat=geo["lat"], lon=geo["lon"])

    return ParcheggioStruttura(
        nome=record.get("name") or record.get("nome", "N/D"),
        tipologia=record.get("tipologia"),
        posti_totali=record.get("posti"),
        tariffa=record.get("tariffa"),
        zona=record.get("nomezona"),
        coordinate=coordinate,
    )


# ---------------------------------------------------------------------------
# Client asincrono
# ---------------------------------------------------------------------------

class SostaBoClient:
    """
    Client asincrono per le API open data del Comune di Bologna.
    Utilizzare come context manager:

        async with SostaBoClient() as client:
            dati = await client.get_disponibilita()
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT):
        self._timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "SostaBoClient":
        self._http = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *_) -> None:
        if self._http:
            await self._http.aclose()

    # -----------------------------------------------------------------------
    # Metodi pubblici
    # -----------------------------------------------------------------------

    async def get_disponibilita(
        self,
        limit: int = 100,
    ) -> list[ParcheggioDisponibilita]:
        """
        Restituisce la disponibilità in tempo reale di tutti i parcheggi.
        I dati vengono aggiornati ogni ~10 minuti dal portale Open Data.
        """
        params = {
            "limit": limit,
            "order_by": "parcheggio",
        }
        data = await self._get(DISPONIBILITA_ENDPOINT, params)
        return [_parse_disponibilita(r) for r in data.get("results", [])]

    async def get_zone(self) -> list[ParcheggioZona]:
        """
        Restituisce i parcheggi di Bologna da OpenStreetMap via Overpass API.
        Fonte: OpenStreetMap contributors (ODbL)
        """
        assert self._http is not None, "SostaBoClient deve essere usato come context manager"
        resp = await self._http.post(
            OVERPASS_URL,
            data={"data": OVERPASS_QUERY},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
        risultati = [_parse_zona_osm(e) for e in elements]
        return [r for r in risultati if r is not None]

    async def get_strutture(
        self,
        limit: int = 100,
    ) -> list[ParcheggioStruttura]:
        """
        Restituisce l'elenco statico delle strutture di sosta di Bologna.
        """
        params = {
            "limit": limit,
            "order_by": "name",
        }
        data = await self._get(STRUTTURE_ENDPOINT, params)
        return [_parse_struttura(r) for r in data.get("results", [])]

    async def ping(self) -> bool:
        """
        Verifica che il portale open data sia raggiungibile.
        Restituisce True se la risposta HTTP è 2xx.
        """
        try:
            resp = await self._http.get(
                DISPONIBILITA_ENDPOINT,
                params={"limit": 1},
            )
            return resp.is_success
        except httpx.RequestError:
            return False

    # -----------------------------------------------------------------------
    # Helper interno
    # -----------------------------------------------------------------------

    async def _get(self, url: str, params: dict) -> dict:
        assert self._http is not None, (
            "SostaBoClient deve essere usato come context manager"
        )
        resp = await self._http.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
