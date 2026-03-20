"""Router SF-001 — CRUD eventi."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from db.bigquery import _table, dml, insert, query
from models.evento import EventoCreate, EventoResponse, PatchEventoRequest
from services.cache import invalidate_lista

router = APIRouter(prefix="/eventi", tags=["eventi"])


def _base_select() -> str:
    """CTE dedup eventi + location dedup + SELECT + JOINs.
    Nessuna WHERE clause — la aggiunge il caller."""
    return f"""
        WITH dedup AS (
          SELECT *
          FROM {_table('EVENTI')}
          QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)) = 1
        ),
        loc_dedup AS (
          SELECT CAST(ID AS INT64) AS id, ANY_VALUE(LOCATION) AS location
          FROM {_table('LOCATION')}
          WHERE ID IS NOT NULL
          GROUP BY ID
        )
        SELECT
          CAST(e.ID AS INT64)             AS id,
          e.DESCRIZIONE                   AS descrizione,
          SUBSTR(e.DATA, 1, 10)           AS data,
          e.ORA_EVENTO                    AS ora_evento,
          CAST(e.STATO AS INT64)          AS stato,
          e.CLIENTE                       AS cliente,
          CAST(e.ID_LOCATION AS INT64)    AS id_location,
          l.location                      AS location_nome,
          CAST(e.TOT_OSPITI AS INT64)           AS tot_ospiti,
          CAST(e.PERC_SEDUTE_APER AS FLOAT64)   AS perc_sedute_aper
        FROM dedup e
        LEFT JOIN loc_dedup l ON l.id = CAST(e.ID_LOCATION AS INT64)
    """


def _base_conditions() -> list[str]:
    """Condizioni di base comuni: escludi cancellati e template."""
    return [
        "COALESCE(CAST(e.DELETED AS INT64), 0) = 0",
        "COALESCE(CAST(e.IS_TEMPLATE AS INT64), 0) = 0",
    ]


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[EventoResponse])
async def list_eventi(
    stato: int | None = Query(
        None,
        description="Filtra per stato: 100=Preventivo, 200=In lavorazione, 400=Confermato. "
                    "Omesso = tutti (esclusi annullati 900).",
    ),
    data_da: str | None = Query(None, description="Data inizio filtro (YYYY-MM-DD)"),
    data_a: str | None = Query(None, description="Data fine filtro (YYYY-MM-DD)"),
    id_location: int | None = Query(None, description="Filtra per location"),
) -> list[EventoResponse]:
    conditions = _base_conditions()
    params: list = []

    if stato is None:
        conditions.append("CAST(e.STATO AS INT64) != 900")
    elif stato == 200:
        conditions.append("CAST(e.STATO AS INT64) IN (200, 300, 350)")
    else:
        conditions.append("CAST(e.STATO AS INT64) = @stato")
        params.append(bigquery.ScalarQueryParameter("stato", "INT64", stato))

    if data_da:
        conditions.append("SAFE_CAST(e.DATA AS DATE) >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da))

    if data_a:
        conditions.append("SAFE_CAST(e.DATA AS DATE) <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a))

    if id_location is not None:
        conditions.append("CAST(e.ID_LOCATION AS INT64) = @id_location")
        params.append(bigquery.ScalarQueryParameter("id_location", "INT64", id_location))

    where = "WHERE " + " AND ".join(conditions)
    sql = _base_select() + where + "\nORDER BY SAFE_CAST(e.DATA AS DATE) ASC"

    rows = await query(sql, params)
    return [EventoResponse(**r) for r in rows]


# ── GET ───────────────────────────────────────────────────────────────────────

@router.get("/{id_evento}", response_model=EventoResponse)
async def get_evento(id_evento: int) -> EventoResponse:
    conditions = _base_conditions() + ["CAST(e.ID AS INT64) = @id"]
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
    id_rows = await query(f"SELECT COALESCE(MAX(CAST(ID AS INT64)), 0) + 1 AS next_id FROM {_table('EVENTI')}")
    new_id = int(id_rows[0]["next_id"])

    await insert("EVENTI", {
        "ID":           new_id,
        "DESCRIZIONE":  body.descrizione,
        "DATA":         body.data.isoformat(),
        "ORA_EVENTO":   body.ora_evento,
        "ID_LOCATION":  body.id_location,
        "STATO":        body.stato,
        "CLIENTE":      body.cliente,
        "DELETED":      0,
        "DISABLED":     0,
        "IS_TEMPLATE":  0,
        "FLG_TEMPLATE": 0,
        "VERS_NUMBER":  0,
        "MAIL_ENABLED": 0,
        "CONTRATTO_FIRMATO": 0,
    })
    return {"id": new_id}


# ── PATCH ─────────────────────────────────────────────────────────────────────

@router.patch("/{id_evento}", response_model=dict)
async def patch_evento(id_evento: int, body: PatchEventoRequest) -> dict:
    """Aggiorna campi evento (stato, ospiti, anagrafica)."""
    set_clauses: list[str] = []
    params: list = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]

    if body.stato is not None:
        set_clauses.append("STATO = @stato")
        params.append(bigquery.ScalarQueryParameter("stato", "INT64", body.stato))
    if body.descrizione is not None:
        set_clauses.append("DESCRIZIONE = @descrizione")
        params.append(bigquery.ScalarQueryParameter("descrizione", "STRING", body.descrizione))
    if body.cliente is not None:
        set_clauses.append("CLIENTE = @cliente")
        params.append(bigquery.ScalarQueryParameter("cliente", "STRING", body.cliente))
    if body.data is not None:
        set_clauses.append("DATA = @data")
        params.append(bigquery.ScalarQueryParameter("data", "STRING", body.data))
    if body.ora_evento is not None:
        set_clauses.append("ORA_EVENTO = @ora_evento")
        params.append(bigquery.ScalarQueryParameter("ora_evento", "STRING", body.ora_evento))
    if body.id_location is not None:
        set_clauses.append("ID_LOCATION = @id_location")
        params.append(bigquery.ScalarQueryParameter("id_location", "INT64", body.id_location))
    if body.tot_ospiti is not None:
        set_clauses.append("TOT_OSPITI = @tot_ospiti")
        params.append(bigquery.ScalarQueryParameter("tot_ospiti", "INT64", body.tot_ospiti))
    if body.perc_sedute_aper is not None:
        set_clauses.append("PERC_SEDUTE_APER = @perc_sedute_aper")
        params.append(bigquery.ScalarQueryParameter("perc_sedute_aper", "FLOAT64", body.perc_sedute_aper))

    if not set_clauses:
        return {"updated": 0}

    affected = await dml(
        f"UPDATE {_table('EVENTI')} SET {', '.join(set_clauses)} "
        f"WHERE CAST(ID AS INT64) = @id",
        params,
    )
    if affected == 0:
        raise HTTPException(404, f"Evento {id_evento} non trovato")
    # Se cambiano ospiti o stato, invalida la cache della lista (ricalcola al prossimo GET)
    if body.tot_ospiti is not None or body.perc_sedute_aper is not None or body.stato is not None:
        invalidate_lista(id_evento)
    return {"updated": id_evento}
