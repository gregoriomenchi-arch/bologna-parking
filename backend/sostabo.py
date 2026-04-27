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
    fonte: Optional[str] = None     # "live" | "static"


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


# ---------------------------------------------------------------------------
# Parcheggi scambiatori / grandi strutture — dati statici con stima oraria
# ---------------------------------------------------------------------------

# (nome, indirizzo, lat, lon, posti_totali, occupazione_base_%, tipo)
# tipo: "scambiatore" | "struttura" | "fiera"
_STATIC_PARCHEGGI_DEF: list[tuple] = [
    # ── SCAMBIATORI (P+Bus, pendolari) ──────────────────────────────────
    ("Parcheggio Tanari",            "Via Luigi Tanari 17",       44.5041, 11.3265, 450, 65, "scambiatore"),
    ("Parcheggio Prati di Caprara",  "Via Prati di Caprara",      44.5082, 11.3085, 400, 55, "scambiatore"),
    ("Parcheggio Antistadio",        "Via Andrea Costa",          44.4918, 11.3064, 283, 50, "scambiatore"),
    ("Parcheggio Ferriera",          "Via della Ferriera",        44.4845, 11.3382, 200, 45, "scambiatore"),
    ("Parcheggio Santa Viola",       "Via Santa Viola",           44.4838, 11.3040, 300, 50, "scambiatore"),
    ("Parcheggio Zaccherini Alvisi", "Via Zaccherini Alvisi",     44.4924, 11.3721, 250, 45, "scambiatore"),
    ("Parcheggio Largo Lercaro",     "Largo Cardinale Lercaro",   44.5042, 11.3388, 300, 55, "scambiatore"),
    ("Parcheggio Piazza della Pace", "Piazza della Pace",         44.4885, 11.3185, 200, 45, "scambiatore"),
    ("Parcheggio Certosa",           "Via MK Gandhi",             44.4935, 11.3268, 325, 40, "scambiatore"),
    ("Parcheggio Ex-Staveco",        "Viale Panzacchi 10",        44.4857, 11.3444, 180, 45, "scambiatore"),
    # ── FIERA (scambiatore con picco eventi) ────────────────────────────
    ("Parcheggio Fiera Sud",         "Piazza Costituzione",       44.5121, 11.3609, 369, 50, "fiera"),
    ("Parcheggio Michelino",         "Via Michelino",             44.5131, 11.3724, 800, 40, "fiera"),
    # ── STRUTTURE CENTRO / SEMICENTRO ───────────────────────────────────
    ("Parcheggio Riva Reno",         "Via del Rondone",           44.4975, 11.3355, 543, 70, "struttura"),
    ("Parcheggio Autostazione",      "Piazza Medaglie d'Oro",     44.5055, 11.3412, 400, 80, "struttura"),
    ("Parcheggio Abycar Stazione",   "Via Fioravanti",            44.5064, 11.3388, 800, 75, "struttura"),
    ("Parcheggio G.T.",              "Via Indipendenza",          44.4992, 11.3457, 500, 70, "struttura"),
    ("Bologna Centrale P1",          "Via Matteotti 5",           44.5067, 11.3435, 477, 80, "struttura"),
    ("Parcheggio Sant'Orsola SABA",  "Via Pietro Albertoni 8",    44.4914, 11.3682, 600, 65, "struttura"),
    ("Parcheggio Salesiani",         "Via Mazzini",               44.5075, 11.3462, 300, 60, "struttura"),
    ("Parcheggio Piazzale Baldi",    "Piazzale Baldi",            44.5034, 11.3518, 200, 55, "struttura"),
    ("Piazzale Atleti Azzurri",      "Piazzale Atleti Azzurri",   44.5028, 11.3710, 350, 50, "struttura"),
]

# Moltiplicatori orari per tipo di parcheggio
_ORA_FACTOR_SCAMBIATORE: list[tuple[int, int, float]] = [
    ( 0,  6, 0.10),   # notte
    ( 6,  7, 0.25),   # pre-mattina
    ( 7,  9, 0.85),   # picco arrivi pendolari
    ( 9, 17, 0.60),   # giornata
    (17, 20, 0.40),   # partenza pendolari — si svuota
    (20, 24, 0.15),   # sera/notte
]

_ORA_FACTOR_STRUTTURA: list[tuple[int, int, float]] = [
    ( 0,  7, 0.15),   # notte
    ( 7,  8, 0.30),   # apertura
    ( 8, 10, 0.55),   # crescita mattutina
    (10, 13, 0.75),   # mattina piena
    (13, 15, 0.90),   # picco pranzo/shopping
    (15, 19, 0.80),   # pomeriggio alto
    (19, 21, 0.55),   # prima sera
    (21, 24, 0.35),   # sera
]


def _ora_factor(hour: int, tipo: str) -> float:
    table = (_ORA_FACTOR_SCAMBIATORE if tipo in ("scambiatore", "fiera")
             else _ORA_FACTOR_STRUTTURA)
    for start, end, factor in table:
        if start <= hour < end:
            return factor
    return 1.0


def get_static_parcheggi() -> list[ParcheggioDisponibilita]:
    """
    Restituisce i 22 parcheggi/scambiatori di Bologna con occupazione stimata
    in base all'ora corrente e al tipo (nessuna chiamata esterna).
    """
    now = datetime.now()
    result = []
    for nome, _indirizzo, lat, lon, totale, base_pct, tipo in _STATIC_PARCHEGGI_DEF:
        factor   = _ora_factor(now.hour, tipo)
        occ_pct  = min(99.0, round(base_pct * factor, 1))
        occupati = round(totale * occ_pct / 100)
        liberi   = totale - occupati
        result.append(ParcheggioDisponibilita(
            nome=nome,
            posti_liberi=liberi,
            posti_occupati=occupati,
            posti_totali=totale,
            occupazione_pct=occ_pct,
            coordinate=Coordinate(lat=lat, lon=lon),
            aggiornato_at=now,
            fonte="static",
        ))
    return result
