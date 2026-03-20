"""Router SF-003 — Scheda Evento.

Tutte le operazioni lavorano sulla cache in memoria.
La persistenza su BigQuery avviene solo con POST /salva.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery

from db.bigquery import _table, query
from models.scheda import (
    AccontoItem,
    AddAccontoRequest,
    AddExtraRequest,
    ExtraItem,
    OspiteItem,
    PatchOspiteRequest,
    PreventivoCalc,
    SchedaResponse,
)
from services.cache import (
    AccontoCached,
    ExtraCached,
    OspiteCached,
    calcola_preventivo,
    get_lista_cache,
    get_scheda_cache,
    get_static,
    invalidate_scheda,
    reload_scheda,
    save_scheda_to_bq,
    _liste,
)

router = APIRouter(prefix="/eventi/{id_evento}/scheda", tags=["scheda"])


async def _check_evento(id_evento: int) -> None:
    """Verifica che l'evento esista. 404 se non trovato."""
    rows = await query(
        f"SELECT CAST(ID AS INT64) AS id FROM {_table('EVENTI')} "
        f"WHERE CAST(ID AS INT64) = @id LIMIT 1",
        [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
    )
    if not rows:
        raise HTTPException(404, f"Evento {id_evento} non trovato")


def _build_response(id_evento: int) -> SchedaResponse:
    """Costruisce SchedaResponse dalla cache (sincrono, dopo aver caricato)."""
    from services.cache import _schede
    scheda = _schede[id_evento]
    # Try to get lista from cache if already loaded
    lista = _liste.get(id_evento)

    import asyncio
    # calcola_preventivo is synchronous pure Python
    from services.cache import _static
    if _static is None:
        raise HTTPException(500, "Cache statica non inizializzata")
    prev_dict = calcola_preventivo(scheda, lista, _static)

    return SchedaResponse(
        ospiti=[
            OspiteItem(
                cod_tipo=o.cod_tipo,
                descrizione=o.descrizione,
                numero=o.numero,
                costo=o.costo,
                sconto=o.sconto,
                note=o.note,
                ordine=o.ordine,
            )
            for o in scheda.ospiti
        ],
        extra=[
            ExtraItem(
                id=e.id,
                descrizione=e.descrizione,
                costo=e.costo,
                quantity=e.quantity,
                ordine=e.ordine,
            )
            for e in scheda.extra
        ],
        acconti=[
            AccontoItem(
                id=a.id,
                acconto=a.acconto,
                data=a.data,
                a_conferma=a.a_conferma,
                descrizione=a.descrizione,
                ordine=a.ordine,
            )
            for a in scheda.acconti
        ],
        preventivo=PreventivoCalc(**prev_dict),
    )


# ── GET ────────────────────────────────────────────────────────────────────────

@router.get("", response_model=SchedaResponse)
async def get_scheda(id_evento: int) -> SchedaResponse:
    """Carica la scheda dalla cache (ricarica da BQ se necessario)."""
    await _check_evento(id_evento)
    await get_scheda_cache(id_evento)
    # Also ensure static is loaded
    await get_static()
    return _build_response(id_evento)


# ── PATCH OSPITE ───────────────────────────────────────────────────────────────

@router.put("/ospiti/{cod_tipo}", response_model=dict)
async def update_ospite(id_evento: int, cod_tipo: str, body: PatchOspiteRequest) -> dict:
    """Aggiorna un tipo ospite nella cache (non scrive su BQ)."""
    scheda = await get_scheda_cache(id_evento)
    ospite = next((o for o in scheda.ospiti if o.cod_tipo == cod_tipo), None)
    if ospite is None:
        raise HTTPException(404, f"Tipo ospite '{cod_tipo}' non trovato")
    ospite.numero = body.numero
    ospite.costo  = body.costo
    ospite.sconto = body.sconto
    ospite.note   = body.note
    scheda.dirty  = True
    return {"updated": cod_tipo}


# ── ADD EXTRA ──────────────────────────────────────────────────────────────────

@router.post("/extra", response_model=ExtraItem, status_code=201)
async def add_extra(id_evento: int, body: AddExtraRequest) -> ExtraItem:
    """Aggiunge un extra alla cache (non scrive su BQ)."""
    scheda = await get_scheda_cache(id_evento)
    item = ExtraCached(
        id=scheda.next_extra_id(),
        descrizione=body.descrizione,
        costo=body.costo,
        quantity=body.quantity,
        ordine=scheda.next_extra_ordine(),
        is_new=True,
    )
    scheda.extra.append(item)
    scheda.dirty = True
    return ExtraItem(
        id=item.id,
        descrizione=item.descrizione,
        costo=item.costo,
        quantity=item.quantity,
        ordine=item.ordine,
    )


# ── DELETE EXTRA ───────────────────────────────────────────────────────────────

@router.delete("/extra/{id_extra}", response_model=dict)
async def delete_extra(id_evento: int, id_extra: int) -> dict:
    """Rimuove un extra dalla cache (non scrive su BQ)."""
    scheda = await get_scheda_cache(id_evento)
    before = len(scheda.extra)
    scheda.extra = [e for e in scheda.extra if e.id != id_extra]
    if len(scheda.extra) == before:
        raise HTTPException(404, f"Extra {id_extra} non trovato")
    scheda.dirty = True
    return {"deleted": id_extra}


# ── ADD ACCONTO ────────────────────────────────────────────────────────────────

@router.post("/acconti", response_model=AccontoItem, status_code=201)
async def add_acconto(id_evento: int, body: AddAccontoRequest) -> AccontoItem:
    """Aggiunge un acconto alla cache (non scrive su BQ)."""
    scheda = await get_scheda_cache(id_evento)
    item = AccontoCached(
        id=scheda.next_acconto_id(),
        acconto=body.acconto,
        data=body.data,
        a_conferma=body.a_conferma,
        descrizione=body.descrizione,
        ordine=scheda.next_acconto_ordine(),
        is_new=True,
    )
    scheda.acconti.append(item)
    scheda.dirty = True
    return AccontoItem(
        id=item.id,
        acconto=item.acconto,
        data=item.data,
        a_conferma=item.a_conferma,
        descrizione=item.descrizione,
        ordine=item.ordine,
    )


# ── DELETE ACCONTO ─────────────────────────────────────────────────────────────

@router.delete("/acconti/{id_acc}", response_model=dict)
async def delete_acconto(id_evento: int, id_acc: int) -> dict:
    """Rimuove un acconto dalla cache (non scrive su BQ)."""
    scheda = await get_scheda_cache(id_evento)
    before = len(scheda.acconti)
    scheda.acconti = [a for a in scheda.acconti if a.id != id_acc]
    if len(scheda.acconti) == before:
        raise HTTPException(404, f"Acconto {id_acc} non trovato")
    scheda.dirty = True
    return {"deleted": id_acc}


# ── SALVA ──────────────────────────────────────────────────────────────────────

@router.post("/salva", response_model=dict)
async def salva_scheda(id_evento: int) -> dict:
    """Persiste la cache su BigQuery."""
    saved = await save_scheda_to_bq(id_evento)
    return {"saved": saved}


# ── RICARICA ───────────────────────────────────────────────────────────────────

@router.post("/ricarica", response_model=SchedaResponse)
async def ricarica_scheda(id_evento: int) -> SchedaResponse:
    """Scarta le modifiche in cache e ricarica da BQ."""
    await _check_evento(id_evento)
    await reload_scheda(id_evento)
    await get_static()
    return _build_response(id_evento)
