"""In-memory cache: tabelle statiche + liste di carico per evento.

Tabelle statiche (TTL 4h, precaricate allo startup):
  ARTICOLI, TB_CODICI_CATEG, TB_TIPI_MAT

Lista di carico (per evento):
  Caricata da BQ al primo accesso, mantenuta in memoria.
  Mutazioni (add/update/delete/recalcola) modificano solo la cache.
  POST /lista/salva scrive su BQ (DELETE + INSERT batch).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from google.cloud import bigquery

from db.bigquery import _table, dml, insert_many, query

logger = logging.getLogger(__name__)

STATIC_TTL = timedelta(hours=4)


# ── Ospiti helpers ─────────────────────────────────────────────────────────────

@dataclass
class OspitiCounts:
    aperitivo: float = 0
    seduto: float = 0
    buffet_dolci: float = 0

    @property
    def totale(self) -> float:
        return self.aperitivo + self.seduto + self.buffet_dolci


def distribuzione_ospiti(tot_ospiti: int | None, perc_sedute_aper: float | None) -> OspitiCounts:
    """Calcola distribuzione ospiti da tot_ospiti + perc — puro Python, no BQ."""
    tot = float(tot_ospiti or 0)
    perc = float(perc_sedute_aper or 0) / 100.0
    n_ape = round(tot * perc) if perc else 0
    return OspitiCounts(aperitivo=n_ape, seduto=tot - n_ape, buffet_dolci=0)


# ── Calcolo quantità — puro Python ────────────────────────────────────────────

def calcola_qta(articolo: dict[str, Any], ospiti: OspitiCounts) -> dict[str, float]:
    """Calcola QTA_APE/SEDU/BUFDOL per un articolo.

    'S' e 'C': COEFF × n_ospiti (fallback a QTA_STD se COEFF = 0).
    'P': totale × PERC_OSPITI / 100.
    """
    flg = (articolo.get("FLG_QTA_TYPE") or "S").upper()

    if flg in ("S", "C"):
        ca = float(articolo.get("COEFF_A") or 0)
        cs = float(articolo.get("COEFF_S") or 0)
        cb = float(articolo.get("COEFF_B") or 0)
        qa = round(ospiti.aperitivo    * ca, 1) if ca else float(articolo.get("QTA_STD_A") or 0)
        qs = round(ospiti.seduto       * cs, 1) if cs else float(articolo.get("QTA_STD_S") or 0)
        qb = round(ospiti.buffet_dolci * cb, 1) if cb else float(articolo.get("QTA_STD_B") or 0)
        return {"qta_ape": qa, "qta_sedu": qs, "qta_bufdol": qb}

    if flg == "P":
        perc = float(articolo.get("PERC_OSPITI") or 100) / 100.0
        totale = round(ospiti.totale * perc)
        return {"qta_ape": totale, "qta_sedu": 0.0, "qta_bufdol": 0.0}

    return {"qta_ape": 0.0, "qta_sedu": 0.0, "qta_bufdol": 0.0}


# ── Tabelle statiche ───────────────────────────────────────────────────────────

@dataclass
class StaticCache:
    articoli: dict[str, dict]    # COD_ARTICOLO → row
    cod_categ: dict[str, str]    # COD_CATEG → COD_TIPO
    tipi_mat: dict[str, dict]    # COD_TIPO → {descrizione, cod_step}
    loaded_at: datetime = field(default_factory=datetime.utcnow)

    def is_stale(self) -> bool:
        return datetime.utcnow() - self.loaded_at > STATIC_TTL

    def get_articolo(self, cod: str) -> dict | None:
        return self.articoli.get(cod)

    def enrich(self, cod_articolo: str) -> dict:
        """Restituisce {descrizione, cod_tipo, tipo_descrizione, cod_step}."""
        art = self.articoli.get(cod_articolo, {})
        cod_categ = str(art.get("COD_CATEG") or "")
        cod_tipo  = self.cod_categ.get(cod_categ) or None
        tipo      = self.tipi_mat.get(cod_tipo or "", {}) if cod_tipo else {}
        return {
            "descrizione":      art.get("DESCRIZIONE"),
            "cod_tipo":         cod_tipo,
            "tipo_descrizione": tipo.get("descrizione"),
            "cod_step":         int(tipo.get("cod_step") or 999),
        }


_static: StaticCache | None = None
_static_lock = asyncio.Lock()


async def get_static() -> StaticCache:
    global _static
    async with _static_lock:
        if _static is None or _static.is_stale():
            _static = await _load_static()
    return _static


async def preload() -> None:
    """Chiamato allo startup: carica le tabelle statiche in memoria."""
    s = await get_static()
    logger.info(
        "Cache statica caricata",
        extra={"articoli": len(s.articoli), "tipi": len(s.tipi_mat)},
    )


async def _load_static() -> StaticCache:
    art_rows, categ_rows, tipo_rows = await asyncio.gather(
        query(f"""
            SELECT * FROM {_table('ARTICOLI')}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY COD_ARTICOLO ORDER BY COD_ARTICOLO) = 1
        """),
        query(f"""
            SELECT COD_CATEG, ANY_VALUE(COD_TIPO) AS COD_TIPO
            FROM {_table('TB_CODICI_CATEG')}
            GROUP BY COD_CATEG
        """),
        query(f"""
            SELECT COD_TIPO,
                   ANY_VALUE(DESCRIZIONE)             AS descrizione,
                   ANY_VALUE(CAST(COD_STEP AS INT64)) AS cod_step
            FROM {_table('TB_TIPI_MAT')}
            GROUP BY COD_TIPO
        """),
    )
    return StaticCache(
        articoli={str(r["COD_ARTICOLO"]): dict(r) for r in art_rows},
        cod_categ={str(r["COD_CATEG"]): str(r["COD_TIPO"]) for r in categ_rows},
        tipi_mat={
            str(r["COD_TIPO"]): {
                "descrizione": r.get("descrizione"),
                "cod_step":    int(r.get("cod_step") or 999),
            }
            for r in tipo_rows
        },
    )


# ── Lista di carico cache ──────────────────────────────────────────────────────

@dataclass
class CachedItem:
    id: int
    cod_articolo: str
    descrizione: str | None
    qta: float
    qta_ape: float
    qta_sedu: float
    qta_bufdol: float
    qta_man_ape: float
    qta_man_sedu: float
    qta_man_bufdol: float
    note: str | None
    colore: str | None
    dimensioni: str | None
    ordine: int
    cod_tipo: str | None
    tipo_descrizione: str | None
    cod_step: int
    is_new: bool = False  # non ancora su BQ


@dataclass
class ListaCache:
    id_evento: int
    items: list[CachedItem]
    _next_ordine: int
    _next_temp_id: int = field(default=-1)
    dirty: bool = False

    def next_ordine(self) -> int:
        v = self._next_ordine
        self._next_ordine += 10
        return v

    def next_temp_id(self) -> int:
        v = self._next_temp_id
        self._next_temp_id -= 1
        return v

    def get(self, item_id: int) -> CachedItem | None:
        return next((i for i in self.items if i.id == item_id), None)

    def add(self, item: CachedItem) -> None:
        self.items.append(item)
        self.dirty = True

    def update_item(self, item_id: int, **changes: Any) -> CachedItem | None:
        item = self.get(item_id)
        if item is None:
            return None
        for k, v in changes.items():
            setattr(item, k, v)
        self.dirty = True
        return item

    def remove(self, item_id: int) -> bool:
        before = len(self.items)
        self.items = [i for i in self.items if i.id != item_id]
        if len(self.items) < before:
            self.dirty = True
            return True
        return False

    def recalcola(self, static: StaticCache, ospiti: OspitiCounts) -> int:
        """Ricalcola quantità di tutti gli articoli in Python. Restituisce n. aggiornati."""
        updated = 0
        for item in self.items:
            art = static.get_articolo(item.cod_articolo)
            if art is None:
                continue
            q = calcola_qta(art, ospiti)
            item.qta_ape    = q["qta_ape"]
            item.qta_sedu   = q["qta_sedu"]
            item.qta_bufdol = q["qta_bufdol"]
            updated += 1
        if updated:
            self.dirty = True
        return updated


_liste: dict[int, ListaCache] = {}
_liste_locks: dict[int, asyncio.Lock] = {}


def _lock(id_evento: int) -> asyncio.Lock:
    if id_evento not in _liste_locks:
        _liste_locks[id_evento] = asyncio.Lock()
    return _liste_locks[id_evento]


async def get_lista_cache(id_evento: int) -> ListaCache:
    async with _lock(id_evento):
        if id_evento not in _liste:
            _liste[id_evento] = await _load_lista_from_bq(id_evento)
    return _liste[id_evento]


def invalidate_lista(id_evento: int) -> None:
    """Forza il ricaricamento dal BQ al prossimo accesso."""
    _liste.pop(id_evento, None)


async def reload_lista(id_evento: int) -> ListaCache:
    """Scarta la cache e ricarica dal BQ (per "Annulla modifiche")."""
    async with _lock(id_evento):
        _liste[id_evento] = await _load_lista_from_bq(id_evento)
    return _liste[id_evento]


async def save_lista_to_bq(id_evento: int) -> int:
    """DELETE + INSERT batch su BQ. Restituisce il numero di righe salvate."""
    cache = _liste.get(id_evento)
    if cache is None or not cache.dirty:
        return 0

    async with _lock(id_evento):
        items = cache.items

        # Assegna ID reali agli item nuovi (is_new=True)
        if any(i.is_new for i in items):
            id_rows = await query(
                f"SELECT COALESCE(MAX(CAST(ID AS INT64)), 0) AS max_id FROM {_table('EVENTI_DET_PREL')}"
            )
            next_id = int(id_rows[0]["max_id"]) + 1
            for item in items:
                if item.is_new:
                    item.id     = next_id
                    item.is_new = False
                    next_id    += 1

        # DELETE tutte le righe dell'evento
        await dml(
            f"DELETE FROM {_table('EVENTI_DET_PREL')} WHERE CAST(ID_EVENTO AS INT64) = @id",
            [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
        )

        # INSERT batch con insert_many (streaming)
        rows = [
            {
                "ID_EVENTO":      id_evento,
                "ID":             item.id,
                "COD_ARTICOLO":   item.cod_articolo,
                "QTA":            item.qta,
                "QTA_APE":        item.qta_ape,
                "QTA_SEDU":       item.qta_sedu,
                "QTA_BUFDOL":     item.qta_bufdol,
                "QTA_MAN_APE":    item.qta_man_ape,
                "QTA_MAN_SEDU":   item.qta_man_sedu,
                "QTA_MAN_BUFDOL": item.qta_man_bufdol,
                "NOTE":           item.note or "",
                "COLORE":         item.colore or "",
                "DIMENSIONI":     item.dimensioni or "",
                "ORDINE":         item.ordine,
            }
            for item in items
        ]
        if rows:
            await insert_many("EVENTI_DET_PREL", rows)

        cache.dirty = False
        logger.info("Lista salvata su BQ", extra={"id_evento": id_evento, "righe": len(rows)})
        return len(rows)


async def _load_lista_from_bq(id_evento: int) -> ListaCache:
    """Carica EVENTI_DET_PREL da BQ, ricalcola QTA da ARTICOLI × ospiti.

    Le quantità automatiche (QTA_APE/SEDU/BUFDOL) vengono sempre ricalcolate
    in Python da ARTICOLI cache × ospiti attuali, replicando il comportamento
    della vista Oracle. I valori stored in BQ vengono usati solo per QTA_MAN_*.
    """
    static = await get_static()
    param = [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)]

    rows, ord_rows, evt_rows = await asyncio.gather(
        query(f"""
            SELECT *
            FROM {_table('EVENTI_DET_PREL')}
            WHERE CAST(ID_EVENTO AS INT64) = @id_evento
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)
            ) = 1
        """, param),
        query(f"""
            SELECT COALESCE(MAX(CAST(ORDINE AS INT64)), 0) + 10 AS next_ordine
            FROM {_table('EVENTI_DET_PREL')}
            WHERE CAST(ID_EVENTO AS INT64) = @id_evento
        """, param),
        query(f"""
            SELECT CAST(TOT_OSPITI AS INT64) AS tot_ospiti,
                   CAST(PERC_SEDUTE_APER AS FLOAT64) AS perc_sedute_aper
            FROM {_table('EVENTI')}
            WHERE CAST(ID AS INT64) = @id_evento
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)) = 1
        """, param),
    )

    next_ordine = int(ord_rows[0]["next_ordine"])

    # Calcola distribuzione ospiti una volta sola
    evt = evt_rows[0] if evt_rows else {}
    ospiti = distribuzione_ospiti(
        tot_ospiti=evt.get("tot_ospiti"),
        perc_sedute_aper=evt.get("perc_sedute_aper"),
    )

    items: list[CachedItem] = []
    for r in sorted(rows, key=lambda x: int(x.get("ORDINE") or 0)):
        cod = str(r.get("COD_ARTICOLO") or "")
        enrich = static.enrich(cod)
        art = static.get_articolo(cod)

        # Ricalcola quantità automatiche da ARTICOLI × ospiti
        if art:
            q = calcola_qta(art, ospiti)
        else:
            q = {"qta_ape": 0.0, "qta_sedu": 0.0, "qta_bufdol": 0.0}

        items.append(CachedItem(
            id=int(r["ID"]),
            cod_articolo=cod,
            descrizione=enrich["descrizione"],
            qta=float(r.get("QTA") or 0),
            qta_ape=q["qta_ape"],
            qta_sedu=q["qta_sedu"],
            qta_bufdol=q["qta_bufdol"],
            qta_man_ape=float(r.get("QTA_MAN_APE") or 0),
            qta_man_sedu=float(r.get("QTA_MAN_SEDU") or 0),
            qta_man_bufdol=float(r.get("QTA_MAN_BUFDOL") or 0),
            note=r.get("NOTE") or None,
            colore=r.get("COLORE") or None,
            dimensioni=r.get("DIMENSIONI") or None,
            ordine=int(r.get("ORDINE") or 0),
            cod_tipo=enrich["cod_tipo"],
            tipo_descrizione=enrich["tipo_descrizione"],
            cod_step=enrich["cod_step"],
        ))

    return ListaCache(id_evento=id_evento, items=items, _next_ordine=next_ordine)
