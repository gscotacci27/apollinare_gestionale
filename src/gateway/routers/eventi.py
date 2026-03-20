"""Router SF-001 — CRUD eventi."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from db.bigquery import _table, dml, insert, query
from models.evento import EventoCreate, EventoResponse

router = APIRouter(prefix="/eventi", tags=["eventi"])


def _base_select() -> str:
    """CTE dedup + SELECT + JOINs. Nessuna WHERE clause — la aggiunge il caller."""
    return f"""
        WITH dedup AS (
          SELECT *
          FROM {_table('EVENTI')}
          QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)) = 1
        )
        SELECT
          CAST(e.ID AS INT64)          AS id,
          e.DESCRIZIONE                AS descrizione,
          e.DATA                       AS data,
          e.ORA_EVENTO                 AS ora_evento,
          CAST(e.STATO AS INT64)       AS stato,
          e.CLIENTE                    AS cliente,
          CAST(e.ID_LOCATION AS INT64) AS id_location,
          l.LOCATION                   AS location_nome,
          CAST(e.TOT_OSPITI AS INT64)  AS tot_ospiti
        FROM dedup e
        LEFT JOIN {_table('LOCATION')} l ON CAST(l.ID AS INT64) = CAST(e.ID_LOCATION AS INT64)
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
