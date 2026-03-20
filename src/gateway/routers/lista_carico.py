"""Router: lista di carico (EVENTI_DET_PREL) — SF-002.

Tutte le operazioni lavorano sulla cache in memoria (services/cache.py).
La persistenza su BigQuery avviene solo con POST /salva.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery

from db.bigquery import _table, query
from models.articolo import AddArticoloRequest, UpdateListaItemRequest
from models.evento import ListaCaricaItem
from services.cache import (
    CachedItem,
    calcola_qta,
    distribuzione_ospiti,
    get_lista_cache,
    get_static,
    invalidate_lista,
    reload_lista,
    save_lista_to_bq,
)

router = APIRouter(prefix="/eventi/{id_evento}/lista", tags=["lista-carico"])


async def _check_confermato(id_evento: int) -> None:
    """Verifica che l'evento sia in stato Confermato (400). Altrimenti 403."""
    rows = await query(
        f"SELECT CAST(STATO AS INT64) AS stato FROM {_table('EVENTI')} "
        f"WHERE CAST(ID AS INT64) = @id LIMIT 1",
        [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
    )
    if not rows:
        raise HTTPException(404, f"Evento {id_evento} non trovato")
    if int(rows[0]["stato"] or 0) != 400:
        raise HTTPException(403, "La lista di carico è accessibile solo per eventi confermati")


def _item_to_response(item: CachedItem) -> ListaCaricaItem:
    return ListaCaricaItem(
        id=item.id,
        cod_articolo=item.cod_articolo,
        descrizione=item.descrizione,
        qta=item.qta,
        qta_ape=item.qta_ape,
        qta_sedu=item.qta_sedu,
        qta_bufdol=item.qta_bufdol,
        qta_man_ape=item.qta_man_ape,
        qta_man_sedu=item.qta_man_sedu,
        qta_man_bufdol=item.qta_man_bufdol,
        note=item.note,
        colore=item.colore,
        dimensioni=item.dimensioni,
        ordine=item.ordine,
        cod_tipo=item.cod_tipo,
        tipo_descrizione=item.tipo_descrizione,
        cod_step=item.cod_step,
    )


# ── GET ────────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ListaCaricaItem])
async def get_lista(id_evento: int) -> list[ListaCaricaItem]:
    """Carica la lista dalla cache (ricarica da BQ se necessario, ricalcolando le quantità)."""
    await _check_confermato(id_evento)
    cache = await get_lista_cache(id_evento)
    return [_item_to_response(i) for i in cache.items]


# ── ADD ────────────────────────────────────────────────────────────────────────

@router.post("", response_model=ListaCaricaItem, status_code=201)
async def add_articolo(id_evento: int, body: AddArticoloRequest) -> ListaCaricaItem:
    """Aggiunge un articolo alla cache (non scrive su BQ)."""
    await _check_confermato(id_evento)

    static = await get_static()
    art = static.get_articolo(body.cod_articolo)
    if art is None:
        raise HTTPException(404, f"Articolo '{body.cod_articolo}' non trovato")

    # Recupera ospiti dall'evento per calcolare quantità
    evt_rows = await query(
        f"""SELECT CAST(TOT_OSPITI AS INT64) AS tot_ospiti,
                   CAST(PERC_SEDUTE_APER AS FLOAT64) AS perc_sedute_aper
            FROM {_table('EVENTI')}
            WHERE CAST(ID AS INT64) = @id LIMIT 1""",
        [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
    )
    evt = evt_rows[0] if evt_rows else {}
    ospiti = distribuzione_ospiti(evt.get("tot_ospiti"), evt.get("perc_sedute_aper"))
    q = calcola_qta(art, ospiti)

    enrich = static.enrich(body.cod_articolo)
    cache  = await get_lista_cache(id_evento)

    item = CachedItem(
        id=cache.next_temp_id(),
        cod_articolo=body.cod_articolo,
        descrizione=enrich["descrizione"],
        qta=1,
        qta_ape=q["qta_ape"],
        qta_sedu=q["qta_sedu"],
        qta_bufdol=q["qta_bufdol"],
        qta_man_ape=float(body.qta_man_ape),
        qta_man_sedu=float(body.qta_man_sedu),
        qta_man_bufdol=float(body.qta_man_bufdol),
        note=body.note,
        colore=None,
        dimensioni=None,
        ordine=cache.next_ordine(),
        cod_tipo=enrich["cod_tipo"],
        tipo_descrizione=enrich["tipo_descrizione"],
        cod_step=enrich["cod_step"],
        is_new=True,
    )
    cache.add(item)
    return _item_to_response(item)


# ── UPDATE ─────────────────────────────────────────────────────────────────────

@router.put("/{item_id}", response_model=dict)
async def update_articolo(id_evento: int, item_id: int, body: UpdateListaItemRequest) -> dict:
    """Aggiorna un articolo nella cache (non scrive su BQ)."""
    cache = await get_lista_cache(id_evento)
    changes: dict = {
        "qta_man_ape":    float(body.qta_man_ape),
        "qta_man_sedu":   float(body.qta_man_sedu),
        "qta_man_bufdol": float(body.qta_man_bufdol),
        "note":           body.note,
        "colore":         body.colore,
        "dimensioni":     body.dimensioni,
    }
    if body.qta_ape    is not None: changes["qta_ape"]    = float(body.qta_ape)
    if body.qta_sedu   is not None: changes["qta_sedu"]   = float(body.qta_sedu)
    if body.qta_bufdol is not None: changes["qta_bufdol"] = float(body.qta_bufdol)

    item = cache.update_item(item_id, **changes)
    if item is None:
        raise HTTPException(404, "Articolo non trovato nella lista")
    return {"updated": item_id}


# ── DELETE ─────────────────────────────────────────────────────────────────────

@router.delete("/{item_id}", response_model=dict)
async def remove_articolo(id_evento: int, item_id: int) -> dict:
    """Rimuove un articolo dalla cache (non scrive su BQ)."""
    await _check_confermato(id_evento)
    cache = await get_lista_cache(id_evento)
    if not cache.remove(item_id):
        raise HTTPException(404, "Articolo non trovato nella lista")
    return {"deleted": item_id}


# ── RECALCOLA ──────────────────────────────────────────────────────────────────

@router.post("/recalcola", response_model=dict)
async def recalcola_lista(id_evento: int) -> dict:
    """Ricalcola tutte le quantità in Python (ARTICOLI cache × ospiti). Nessuna query BQ."""
    await _check_confermato(id_evento)

    static = await get_static()
    evt_rows = await query(
        f"""SELECT CAST(TOT_OSPITI AS INT64) AS tot_ospiti,
                   CAST(PERC_SEDUTE_APER AS FLOAT64) AS perc_sedute_aper
            FROM {_table('EVENTI')}
            WHERE CAST(ID AS INT64) = @id LIMIT 1""",
        [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
    )
    evt    = evt_rows[0] if evt_rows else {}
    ospiti = distribuzione_ospiti(evt.get("tot_ospiti"), evt.get("perc_sedute_aper"))

    cache   = await get_lista_cache(id_evento)
    updated = cache.recalcola(static, ospiti)
    return {"recalculated": updated}


# ── SALVA ──────────────────────────────────────────────────────────────────────

@router.post("/salva", response_model=dict)
async def salva_lista(id_evento: int) -> dict:
    """Persiste la cache su BigQuery (DELETE + INSERT batch)."""
    await _check_confermato(id_evento)
    saved = await save_lista_to_bq(id_evento)
    return {"saved": saved}


# ── RICARICA ───────────────────────────────────────────────────────────────────

@router.post("/ricarica", response_model=list[ListaCaricaItem])
async def ricarica_lista(id_evento: int) -> list[ListaCaricaItem]:
    """Scarta le modifiche in cache e ricarica da BQ (ricalcolando le quantità)."""
    await _check_confermato(id_evento)
    cache = await reload_lista(id_evento)
    return [_item_to_response(i) for i in cache.items]
