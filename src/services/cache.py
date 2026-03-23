"""In-memory cache: tabelle statiche + liste di carico per evento.

Tabelle statiche (TTL 4h, precaricate allo startup):
  articoli, tb_codici_categ, tb_tipi_mat

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

# Mapping cod_tipo ospiti → prefisso colonne in eventi
_OSPITI_MAP: dict[str, str] = {
    "8": "adulti",
    "5": "bambini",
    "7": "fornitori",
    "6": "altri",
}


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
    flg = (articolo.get("flg_qta_type") or "S").upper()

    if flg in ("S", "C"):
        ca = float(articolo.get("coeff_a") or 0)
        cs = float(articolo.get("coeff_s") or 0)
        cb = float(articolo.get("coeff_b") or 0)
        qa = round(ospiti.aperitivo    * ca, 1) if ca else float(articolo.get("qta_std_a") or 0)
        qs = round(ospiti.seduto       * cs, 1) if cs else float(articolo.get("qta_std_s") or 0)
        qb = round(ospiti.buffet_dolci * cb, 1) if cb else float(articolo.get("qta_std_b") or 0)
        return {"qta_ape": qa, "qta_sedu": qs, "qta_bufdol": qb}

    if flg == "P":
        perc = float(articolo.get("perc_ospiti") or 100) / 100.0
        totale = round(ospiti.totale * perc)
        return {"qta_ape": totale, "qta_sedu": 0.0, "qta_bufdol": 0.0}

    return {"qta_ape": 0.0, "qta_sedu": 0.0, "qta_bufdol": 0.0}


# ── Tabelle statiche ───────────────────────────────────────────────────────────

@dataclass
class StaticCache:
    articoli: dict[str, dict]          # cod_articolo → row
    cod_categ: dict[str, str]          # cod_categ → cod_tipo
    tipi_mat: dict[str, dict]          # cod_tipo → {descrizione, cod_step}
    tipi_ospiti: dict[str, str]        # cod_tipo → descrizione
    costi_articoli: dict[str, float]   # cod_articolo → prezzo_netto
    loaded_at: datetime = field(default_factory=datetime.utcnow)

    def is_stale(self) -> bool:
        return datetime.utcnow() - self.loaded_at > STATIC_TTL

    def get_articolo(self, cod: str) -> dict | None:
        return self.articoli.get(cod)

    def enrich(self, cod_articolo: str) -> dict:
        """Restituisce {descrizione, cod_tipo, tipo_descrizione, cod_step}."""
        art = self.articoli.get(cod_articolo, {})
        cod_categ = str(art.get("cod_categ") or "")
        cod_tipo  = self.cod_categ.get(cod_categ) or None
        tipo      = self.tipi_mat.get(cod_tipo or "", {}) if cod_tipo else {}
        return {
            "descrizione":      art.get("descrizione"),
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
    art_rows, categ_rows, tipo_rows, tipi_ospiti_rows, costi_rows = await asyncio.gather(
        query(f"SELECT * FROM {_table('articoli')}"),
        query(f"SELECT cod_categ, cod_tipo FROM {_table('tb_codici_categ')}"),
        query(f"SELECT cod_tipo, descrizione, cod_step FROM {_table('tb_tipi_mat')}"),
        query(f"SELECT cod_tipo, descrizione FROM {_table('tb_tipi_ospiti')}"),
        query(f"""
            SELECT p.cod_articolo, p.prezzo_netto
            FROM {_table('prezzi_listino')} p
            INNER JOIN (
                SELECT cod_articolo, MAX(valido_dal) AS max_data
                FROM {_table('prezzi_listino')}
                WHERE prezzo_netto IS NOT NULL
                GROUP BY cod_articolo
            ) latest ON latest.cod_articolo = p.cod_articolo
                     AND latest.max_data = p.valido_dal
            WHERE p.prezzo_netto IS NOT NULL
        """),
    )
    return StaticCache(
        articoli={str(r["cod_articolo"]): dict(r) for r in art_rows},
        cod_categ={str(r["cod_categ"]): str(r["cod_tipo"]) for r in categ_rows},
        tipi_mat={
            str(r["cod_tipo"]): {
                "descrizione": r.get("descrizione"),
                "cod_step":    int(r.get("cod_step") or 999),
            }
            for r in tipo_rows
        },
        tipi_ospiti={str(r["cod_tipo"]): str(r.get("descrizione") or "") for r in tipi_ospiti_rows},
        costi_articoli={str(r["cod_articolo"]): float(r["prezzo_netto"] or 0) for r in costi_rows},
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


def invalidate_lista_all() -> None:
    """Invalida tutte le liste (usato quando cambiano articoli o sezioni)."""
    _liste.clear()


def invalidate_static() -> None:
    """Forza il ricaricamento della cache statica al prossimo accesso."""
    global _static
    _static = None


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
                f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {_table('eventi_det_prel')}"
            )
            next_id = int(id_rows[0]["max_id"]) + 1
            for item in items:
                if item.is_new:
                    item.id     = next_id
                    item.is_new = False
                    next_id    += 1

        # DELETE tutte le righe dell'evento
        await dml(
            f"DELETE FROM {_table('eventi_det_prel')} WHERE id_evento = @id",
            [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
        )

        # INSERT batch con insert_many (streaming)
        rows = [
            {
                "id_evento":      id_evento,
                "id":             item.id,
                "cod_articolo":   item.cod_articolo,
                "qta":            item.qta,
                "qta_ape":        item.qta_ape,
                "qta_sedu":       item.qta_sedu,
                "qta_bufdol":     item.qta_bufdol,
                "qta_man_ape":    item.qta_man_ape,
                "qta_man_sedu":   item.qta_man_sedu,
                "qta_man_bufdol": item.qta_man_bufdol,
                "note":           item.note or "",
                "colore":         item.colore or "",
                "dimensioni":     item.dimensioni or "",
                "ordine":         item.ordine,
            }
            for item in items
        ]
        if rows:
            await insert_many("eventi_det_prel", rows)

        cache.dirty = False
        logger.info("Lista salvata su BQ", extra={"id_evento": id_evento, "righe": len(rows)})
        return len(rows)


async def _load_lista_from_bq(id_evento: int) -> ListaCache:
    """Carica eventi_det_prel da BQ, ricalcola QTA da articoli × ospiti.

    Le quantità automatiche (qta_ape/sedu/bufdol) vengono sempre ricalcolate
    in Python da articoli cache × ospiti attuali, replicando il comportamento
    della vista Oracle. I valori stored in BQ vengono usati solo per qta_man_*.
    """
    static = await get_static()
    param = [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)]

    rows, ord_rows, evt_rows = await asyncio.gather(
        query(f"""
            SELECT *
            FROM {_table('eventi_det_prel')}
            WHERE id_evento = @id_evento
        """, param),
        query(f"""
            SELECT COALESCE(MAX(ordine), 0) + 10 AS next_ordine
            FROM {_table('eventi_det_prel')}
            WHERE id_evento = @id_evento
        """, param),
        query(f"""
            SELECT
              COALESCE(n_adulti,0) + COALESCE(n_bambini,0)
                + COALESCE(n_fornitori,0) + COALESCE(n_altri,0) AS tot_ospiti,
              perc_sedute_aper
            FROM {_table('eventi')}
            WHERE id = @id_evento
            LIMIT 1
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
    for r in sorted(rows, key=lambda x: int(x.get("ordine") or 0)):
        cod = str(r.get("cod_articolo") or "")
        enrich = static.enrich(cod)
        art = static.get_articolo(cod)

        # Ricalcola quantità automatiche da articoli × ospiti
        if art:
            q = calcola_qta(art, ospiti)
        else:
            q = {"qta_ape": 0.0, "qta_sedu": 0.0, "qta_bufdol": 0.0}

        items.append(CachedItem(
            id=int(r["id"]),
            cod_articolo=cod,
            descrizione=enrich["descrizione"],
            qta=float(r.get("qta") or 0),
            qta_ape=q["qta_ape"],
            qta_sedu=q["qta_sedu"],
            qta_bufdol=q["qta_bufdol"],
            qta_man_ape=float(r.get("qta_man_ape") or 0),
            qta_man_sedu=float(r.get("qta_man_sedu") or 0),
            qta_man_bufdol=float(r.get("qta_man_bufdol") or 0),
            note=r.get("note") or None,
            colore=r.get("colore") or None,
            dimensioni=r.get("dimensioni") or None,
            ordine=int(r.get("ordine") or 0),
            cod_tipo=enrich["cod_tipo"],
            tipo_descrizione=enrich["tipo_descrizione"],
            cod_step=enrich["cod_step"],
        ))

    return ListaCache(id_evento=id_evento, items=items, _next_ordine=next_ordine)


# ── Scheda evento cache ────────────────────────────────────────────────────────

@dataclass
class OspiteCached:
    cod_tipo: str
    descrizione: str | None
    numero: int
    costo: float
    sconto: float
    note: str | None
    ordine: int


@dataclass
class ExtraCached:
    id: int
    descrizione: str
    costo: float
    quantity: float
    ordine: int
    is_new: bool = False


@dataclass
class AccontoCached:
    id: int
    acconto: float
    data: str | None          # ISO date
    a_conferma: int
    descrizione: str | None
    ordine: int
    is_new: bool = False


@dataclass
class DegustCached:
    id: int
    data: str | None
    nome: str | None
    n_persone: int
    costo_degustazione: float
    detraibile: int
    consumata: int            # 0=da_programmare, 1=effettuata
    note: str | None
    is_new: bool = False


@dataclass
class SchedaCache:
    id_evento: int
    ospiti: list[OspiteCached]
    extra: list[ExtraCached]
    acconti: list[AccontoCached]
    degustazioni: list[DegustCached] = field(default_factory=list)
    sconto_totale: float = 0.0
    totale_manuale: float | None = None
    _next_extra_id: int = field(default=-1)
    _next_acconto_id: int = field(default=-1)
    _next_degust_id: int = field(default=-1)
    _next_extra_ordine: int = field(default=10)
    _next_acconto_ordine: int = field(default=10)
    dirty: bool = False

    def next_extra_id(self) -> int:
        v = self._next_extra_id
        self._next_extra_id -= 1
        return v

    def next_acconto_id(self) -> int:
        v = self._next_acconto_id
        self._next_acconto_id -= 1
        return v

    def next_degust_id(self) -> int:
        v = self._next_degust_id
        self._next_degust_id -= 1
        return v

    def next_extra_ordine(self) -> int:
        v = self._next_extra_ordine
        self._next_extra_ordine += 10
        return v

    def next_acconto_ordine(self) -> int:
        v = self._next_acconto_ordine
        self._next_acconto_ordine += 10
        return v


_schede: dict[int, SchedaCache] = {}
_schede_locks: dict[int, asyncio.Lock] = {}


def _scheda_lock(id_evento: int) -> asyncio.Lock:
    if id_evento not in _schede_locks:
        _schede_locks[id_evento] = asyncio.Lock()
    return _schede_locks[id_evento]


async def get_scheda_cache(id_evento: int) -> SchedaCache:
    async with _scheda_lock(id_evento):
        if id_evento not in _schede:
            _schede[id_evento] = await _load_scheda_from_bq(id_evento)
    return _schede[id_evento]


def invalidate_scheda(id_evento: int) -> None:
    """Forza il ricaricamento dal BQ al prossimo accesso."""
    _schede.pop(id_evento, None)


async def reload_scheda(id_evento: int) -> SchedaCache:
    """Scarta la cache e ricarica dal BQ."""
    async with _scheda_lock(id_evento):
        _schede[id_evento] = await _load_scheda_from_bq(id_evento)
    return _schede[id_evento]


async def save_scheda_to_bq(id_evento: int) -> bool:
    """Salva ospiti (UPDATE eventi), extra, acconti e degustazioni su BQ."""
    scheda = _schede.get(id_evento)
    if scheda is None:
        return False

    async with _scheda_lock(id_evento):
        param = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]

        # ── OSPITI → UPDATE eventi (colonne pivotate) ──────────────────────────
        set_clauses: list[str] = []
        update_params: list = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]
        for o in scheda.ospiti:
            prefix = _OSPITI_MAP.get(str(o.cod_tipo))
            if prefix is None:
                continue
            set_clauses += [
                f"n_{prefix} = @n_{prefix}",
                f"costo_{prefix} = @costo_{prefix}",
                f"sconto_{prefix} = @sconto_{prefix}",
            ]
            update_params += [
                bigquery.ScalarQueryParameter(f"n_{prefix}", "INT64", o.numero),
                bigquery.ScalarQueryParameter(f"costo_{prefix}", "FLOAT64", o.costo),
                bigquery.ScalarQueryParameter(f"sconto_{prefix}", "FLOAT64", o.sconto),
            ]
        # Aggiungi anche sconto_totale e totale_manuale nella stessa UPDATE
        set_clauses += ["sconto_totale = @sconto", "totale_manuale = @totale_manuale"]
        update_params += [
            bigquery.ScalarQueryParameter("sconto", "FLOAT64", scheda.sconto_totale),
            bigquery.ScalarQueryParameter("totale_manuale", "FLOAT64", scheda.totale_manuale),
        ]
        if set_clauses:
            await dml(
                f"UPDATE {_table('eventi')} SET {', '.join(set_clauses)} WHERE id = @id",
                update_params,
            )

        # ── EVENTO_EXTRA ───────────────────────────────────────────────────────
        if any(e.is_new for e in scheda.extra):
            id_rows = await query(
                f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {_table('evento_extra')}"
            )
            next_id = int(id_rows[0]["max_id"]) + 1
            for e in scheda.extra:
                if e.is_new:
                    e.id = next_id
                    e.is_new = False
                    next_id += 1

        await dml(
            f"DELETE FROM {_table('evento_extra')} WHERE id_evento = @id",
            param,
        )
        extra_rows = [
            {
                "id":          e.id,
                "id_evento":   id_evento,
                "descrizione": e.descrizione,
                "costo":       e.costo,
                "quantity":    e.quantity,
                "ordine":      e.ordine,
            }
            for e in scheda.extra
        ]
        if extra_rows:
            await insert_many("evento_extra", extra_rows)

        # ── EVENTO_ACCONTI ─────────────────────────────────────────────────────
        if any(a.is_new for a in scheda.acconti):
            id_rows = await query(
                f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {_table('evento_acconti')}"
            )
            next_id = int(id_rows[0]["max_id"]) + 1
            for a in scheda.acconti:
                if a.is_new:
                    a.id = next_id
                    a.is_new = False
                    next_id += 1

        await dml(
            f"DELETE FROM {_table('evento_acconti')} WHERE id_evento = @id",
            param,
        )
        acconti_rows = [
            {
                "id":            a.id,
                "id_evento":     id_evento,
                "importo":       a.acconto,
                "data_scadenza": a.data or None,
                "is_conferma":   bool(a.a_conferma),
                "descrizione":   a.descrizione or "",
                "ordine":        a.ordine,
            }
            for a in scheda.acconti
        ]
        if acconti_rows:
            await insert_many("evento_acconti", acconti_rows)

        # ── EVENTO_DEGUST ──────────────────────────────────────────────────────
        if any(d.is_new for d in scheda.degustazioni):
            id_rows = await query(
                f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {_table('evento_degust')}"
            )
            next_id = int(id_rows[0]["max_id"]) + 1
            for d in scheda.degustazioni:
                if d.is_new:
                    d.id = next_id
                    d.is_new = False
                    next_id += 1

        await dml(
            f"DELETE FROM {_table('evento_degust')} WHERE id_evento = @id",
            param,
        )
        degust_rows = [
            {
                "id":         d.id,
                "id_evento":  id_evento,
                "stato":      "effettuata" if d.consumata else "da_programmare",
                "data":       d.data or None,
                "nome":       d.nome or "",
                "n_persone":  d.n_persone,
                "costo":      d.costo_degustazione,
                "detraibile": bool(d.detraibile),
                "note":       d.note or "",
            }
            for d in scheda.degustazioni
        ]
        if degust_rows:
            await insert_many("evento_degust", degust_rows)

        scheda.dirty = False
        logger.info("Scheda salvata su BQ", extra={"id_evento": id_evento})
        return True


def calcola_preventivo(
    scheda: SchedaCache,
    lista: "ListaCache | None",
    static: StaticCache,
) -> dict:
    """Calcola il preventivo in puro Python."""
    ospiti_subtotale = sum(
        o.numero * o.costo * (1.0 - o.sconto / 100.0)
        for o in scheda.ospiti
    )

    articoli_subtotale = 0.0
    if lista is not None:
        for item in lista.items:
            costo_uni = static.costi_articoli.get(item.cod_articolo, 0.0)
            qta = (
                item.qta_ape + item.qta_sedu + item.qta_bufdol
                + item.qta_man_ape + item.qta_man_sedu + item.qta_man_bufdol
            )
            articoli_subtotale += costo_uni * qta

    extra_subtotale = sum(e.costo * e.quantity for e in scheda.extra)

    degustazioni_detraibili = sum(
        d.costo_degustazione for d in scheda.degustazioni if d.detraibile
    )

    totale_calc = (
        ospiti_subtotale + articoli_subtotale + extra_subtotale
        - degustazioni_detraibili - scheda.sconto_totale
    )
    totale_netto = scheda.totale_manuale if scheda.totale_manuale is not None else totale_calc

    acconti_totale = sum(a.acconto for a in scheda.acconti)
    saldo = totale_netto - acconti_totale

    return {
        "ospiti_subtotale":        round(ospiti_subtotale, 2),
        "articoli_subtotale":      round(articoli_subtotale, 2),
        "extra_subtotale":         round(extra_subtotale, 2),
        "degustazioni_detraibili": round(degustazioni_detraibili, 2),
        "sconto_totale":           round(scheda.sconto_totale, 2),
        "totale_netto":            round(totale_netto, 2),
        "totale_manuale":          round(scheda.totale_manuale, 2) if scheda.totale_manuale is not None else None,
        "acconti_totale":          round(acconti_totale, 2),
        "saldo":                   round(saldo, 2),
    }


async def _load_scheda_from_bq(id_evento: int) -> SchedaCache:
    """Carica ospiti (da eventi), extra, acconti e degustazioni da BQ."""
    static = await get_static()
    param = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]

    (
        extra_rows, acconti_rows, max_extra_id_rows, max_acc_id_rows,
        degust_rows, evento_rows,
    ) = await asyncio.gather(
        query(f"""
            SELECT id, descrizione, costo, quantity, ordine
            FROM {_table('evento_extra')}
            WHERE id_evento = @id
            ORDER BY ordine
        """, param),
        query(f"""
            SELECT id, importo, data_scadenza, is_conferma, descrizione, ordine
            FROM {_table('evento_acconti')}
            WHERE id_evento = @id
            ORDER BY ordine
        """, param),
        query(f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {_table('evento_extra')}"),
        query(f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {_table('evento_acconti')}"),
        query(f"""
            SELECT id, stato, data, nome, n_persone, costo, detraibile, note
            FROM {_table('evento_degust')}
            WHERE id_evento = @id
            ORDER BY id
        """, param),
        query(f"""
            SELECT
              n_adulti, costo_adulti, sconto_adulti,
              n_bambini, costo_bambini, sconto_bambini,
              n_fornitori, costo_fornitori, sconto_fornitori,
              n_altri, costo_altri, sconto_altri,
              COALESCE(sconto_totale, 0) AS sconto_totale,
              totale_manuale
            FROM {_table('eventi')}
            WHERE id = @id
            LIMIT 1
        """, param),
    )

    # ── Ospiti da colonne pivotate di eventi ───────────────────────────────────
    evt = evento_rows[0] if evento_rows else {}
    ospiti_map_ordered = [("8", "adulti"), ("5", "bambini"), ("7", "fornitori"), ("6", "altri")]
    ospiti = [
        OspiteCached(
            cod_tipo=cod_tipo,
            descrizione=static.tipi_ospiti.get(cod_tipo),
            numero=int(evt.get(f"n_{prefix}") or 0),
            costo=float(evt.get(f"costo_{prefix}") or 0),
            sconto=float(evt.get(f"sconto_{prefix}") or 0),
            note=None,
            ordine=idx * 10,
        )
        for idx, (cod_tipo, prefix) in enumerate(ospiti_map_ordered)
    ]

    extra = [
        ExtraCached(
            id=int(r["id"]),
            descrizione=str(r.get("descrizione") or ""),
            costo=float(r.get("costo") or 0),
            quantity=float(r.get("quantity") or 1),
            ordine=int(r.get("ordine") or 0),
        )
        for r in extra_rows
    ]

    acconti = [
        AccontoCached(
            id=int(r["id"]),
            acconto=float(r.get("importo") or 0),
            data=str(r["data_scadenza"])[:10] if r.get("data_scadenza") else None,
            a_conferma=1 if r.get("is_conferma") else 0,
            descrizione=r.get("descrizione") or None,
            ordine=int(r.get("ordine") or 0),
        )
        for r in acconti_rows
    ]

    degustazioni = [
        DegustCached(
            id=int(r["id"]),
            data=r.get("data") or None,
            nome=r.get("nome") or None,
            n_persone=int(r.get("n_persone") or 0),
            costo_degustazione=float(r.get("costo") or 0),
            detraibile=1 if r.get("detraibile") else 0,
            consumata=1 if r.get("stato") == "effettuata" else 0,
            note=r.get("note") or None,
        )
        for r in degust_rows
    ]

    sconto_totale = float(evt.get("sconto_totale") or 0)
    totale_manuale = float(evt["totale_manuale"]) if evt.get("totale_manuale") is not None else None

    next_extra_ord = (max(e.ordine for e in extra) + 10) if extra else 10
    next_acc_ord = (max(a.ordine for a in acconti) + 10) if acconti else 10

    return SchedaCache(
        id_evento=id_evento,
        ospiti=ospiti,
        extra=extra,
        acconti=acconti,
        degustazioni=degustazioni,
        sconto_totale=sconto_totale,
        totale_manuale=totale_manuale,
        _next_extra_id=int(max_extra_id_rows[0]["max_id"]) - 1,
        _next_acconto_id=int(max_acc_id_rows[0]["max_id"]) - 1,
        _next_degust_id=-1,
        _next_extra_ordine=next_extra_ord,
        _next_acconto_ordine=next_acc_ord,
    )
