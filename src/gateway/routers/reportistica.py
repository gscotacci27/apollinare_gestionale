"""Router: reportistica (consuntivo, impegni magazzino, acconti in scadenza)."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query
from google.cloud import bigquery

from db.bigquery import _table, query

router = APIRouter(prefix="/report", tags=["reportistica"])


@router.get("/calendario")
async def calendario(
    data_da: date | None = None,
    data_a: date | None = None,
):
    conditions = []
    params: list = []
    if data_da:
        conditions.append("data >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da.isoformat()))
    if data_a:
        conditions.append("data <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a.isoformat()))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return await query(f"SELECT * FROM {_table('VW_CALENDARIO_EVENTI')} {where} ORDER BY data", params)


@router.get("/consuntivo/{id_evento}")
async def consuntivo(id_evento: int):
    return await query(f"""
        SELECT * FROM {_table('GET_REPORT_CONSUNTIVO_PER_DATA')}
        WHERE id = @id
        ORDER BY ordine
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])


@router.get("/impegni-magazzino")
async def impegni_magazzino(
    data_da: date | None = None,
    data_a: date | None = None,
    cod_articolo: str | None = None,
):
    conditions = []
    params: list = []
    if data_da:
        conditions.append("data >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da.isoformat()))
    if data_a:
        conditions.append("data <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a.isoformat()))
    if cod_articolo:
        conditions.append("cod_articolo = @cod")
        params.append(bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return await query(f"SELECT * FROM {_table('V_IMPEGNI_ARTICOLI_LOC')} {where}", params)


@router.get("/acconti-in-scadenza")
async def acconti_in_scadenza():
    """Eventi con seconda caparra non versata entro 65 giorni."""
    return await query(f"SELECT * FROM {_table('GET_EVENTI_DA_PAGARE_ENTRO_65GG')}")


@router.get("/costi-per-tipo/{id_evento}")
async def costi_per_tipo(id_evento: int):
    return await query(f"""
        SELECT t.cod_tipo, m.descrizione, t.numero, t.costo, t.costo_ivato
        FROM {_table('GET_COSTO_TIPI_EVT')} t
        JOIN {_table('TB_TIPI_MAT')} m ON m.cod_tipo = t.cod_tipo
        WHERE t.id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
