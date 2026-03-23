"""Router SF-001 — CRUD eventi."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from db.bigquery import _table, dml, insert, query
from models.evento import EventoCreate, EventoResponse, PatchEventoRequest
from services.cache import invalidate_lista, invalidate_scheda

router = APIRouter(prefix="/eventi", tags=["eventi"])

# Mapping stati stringa → gruppo per filtro "in_lavorazione"
_STATI_LAVORAZIONE = ("in_lavorazione",)


def _base_select() -> str:
    """SELECT eventi + JOIN location + cliente. No WHERE clause."""
    return f"""
        SELECT
          e.id                                                      AS id,
          e.descrizione                                             AS descrizione,
          e.data                                                    AS data,
          e.ora_evento                                              AS ora_evento,
          e.stato                                                   AS stato,
          c.nome                                                    AS cliente,
          e.id_location                                             AS id_location,
          l.nome                                                    AS location_nome,
          (COALESCE(e.n_adulti,0) + COALESCE(e.n_bambini,0)
           + COALESCE(e.n_fornitori,0) + COALESCE(e.n_altri,0))   AS tot_ospiti,
          e.perc_sedute_aper                                        AS perc_sedute_aper
        FROM {_table('eventi')} e
        LEFT JOIN {_table('location')} l ON l.id = e.id_location
        LEFT JOIN {_table('clienti')} c ON c.id = e.id_cliente
    """


def _base_conditions() -> list[str]:
    return [
        "COALESCE(e.deleted, FALSE) = FALSE",
    ]


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[EventoResponse])
async def list_eventi(
    stato: str | None = Query(
        None,
        description="Filtra per stato: in_attesa_conferma, in_lavorazione, confermato. "
                    "Omesso = tutti (esclusi annullati).",
    ),
    data_da: str | None = Query(None, description="Data inizio filtro (YYYY-MM-DD)"),
    data_a: str | None = Query(None, description="Data fine filtro (YYYY-MM-DD)"),
    id_location: int | None = Query(None, description="Filtra per location"),
) -> list[EventoResponse]:
    conditions = _base_conditions()
    params: list = []

    if stato is None:
        conditions.append("e.stato != 'annullato'")
    else:
        conditions.append("e.stato = @stato")
        params.append(bigquery.ScalarQueryParameter("stato", "STRING", stato))

    if data_da:
        conditions.append("SAFE_CAST(e.data AS DATE) >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da))
    if data_a:
        conditions.append("SAFE_CAST(e.data AS DATE) <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a))
    if id_location is not None:
        conditions.append("e.id_location = @id_location")
        params.append(bigquery.ScalarQueryParameter("id_location", "INT64", id_location))

    where = "WHERE " + " AND ".join(conditions)
    sql = _base_select() + where + "\nORDER BY SAFE_CAST(e.data AS DATE) ASC"

    rows = await query(sql, params)
    return [EventoResponse(**r) for r in rows]


# ── GET ───────────────────────────────────────────────────────────────────────

@router.get("/{id_evento}", response_model=EventoResponse)
async def get_evento(id_evento: int) -> EventoResponse:
    conditions = _base_conditions() + ["e.id = @id"]
    where = "WHERE " + " AND ".join(conditions)
    rows = await query(
        _base_select() + where,
        [bigquery.ScalarQueryParameter("id", "INT64", id_evento)],
    )
    if not rows:
        raise HTTPException(404, f"Evento {id_evento} non trovato")
    return EventoResponse(**rows[0])


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post("", response_model=dict, status_code=201)
async def create_evento(body: EventoCreate) -> dict:
    id_rows = await query(
        f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {_table('eventi')}"
    )
    new_id = int(id_rows[0]["next_id"])

    await insert("eventi", {
        "id":           new_id,
        "descrizione":  body.descrizione,
        "data":         body.data.isoformat(),
        "ora_evento":   body.ora_evento,
        "id_location":  body.id_location,
        "stato":        body.stato,
        "source":       "web",
        "deleted":      False,
        "mail_enabled": False,
        "contratto_firmato": False,
        "created_at":   None,
        "updated_at":   None,
    })
    return {"id": new_id}


# ── PATCH ─────────────────────────────────────────────────────────────────────

@router.patch("/{id_evento}", response_model=dict)
async def patch_evento(id_evento: int, body: PatchEventoRequest) -> dict:
    set_clauses: list[str] = []
    params: list = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]

    if body.stato is not None:
        set_clauses.append("stato = @stato")
        params.append(bigquery.ScalarQueryParameter("stato", "STRING", body.stato))
    if body.descrizione is not None:
        set_clauses.append("descrizione = @descrizione")
        params.append(bigquery.ScalarQueryParameter("descrizione", "STRING", body.descrizione))
    if body.data is not None:
        set_clauses.append("data = @data")
        params.append(bigquery.ScalarQueryParameter("data", "STRING", body.data))
    if body.ora_evento is not None:
        set_clauses.append("ora_evento = @ora_evento")
        params.append(bigquery.ScalarQueryParameter("ora_evento", "STRING", body.ora_evento))
    if body.id_location is not None:
        set_clauses.append("id_location = @id_location")
        params.append(bigquery.ScalarQueryParameter("id_location", "INT64", body.id_location))
    if body.perc_sedute_aper is not None:
        set_clauses.append("perc_sedute_aper = @perc_sedute_aper")
        params.append(bigquery.ScalarQueryParameter("perc_sedute_aper", "FLOAT64", body.perc_sedute_aper))

    if not set_clauses:
        return {"updated": 0}

    affected = await dml(
        f"UPDATE {_table('eventi')} SET {', '.join(set_clauses)} WHERE id = @id",
        params,
    )
    if affected == 0:
        raise HTTPException(404, f"Evento {id_evento} non trovato")

    if body.perc_sedute_aper is not None or body.stato is not None:
        invalidate_lista(id_evento)
        invalidate_scheda(id_evento)
    return {"updated": id_evento}
