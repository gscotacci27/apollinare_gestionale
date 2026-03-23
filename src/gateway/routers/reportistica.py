"""Router: reportistica (consuntivo, impegni magazzino, acconti in scadenza)."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter
from google.cloud import bigquery

from db.bigquery import _table, query

router = APIRouter(prefix="/report", tags=["reportistica"])


@router.get("/calendario")
async def calendario(
    data_da: date | None = None,
    data_a: date | None = None,
):
    conditions = [
        "COALESCE(e.deleted, FALSE) = FALSE",
        "e.stato != 'annullato'",
    ]
    params: list = []
    if data_da:
        conditions.append("SAFE_CAST(e.data AS DATE) >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da.isoformat()))
    if data_a:
        conditions.append("SAFE_CAST(e.data AS DATE) <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a.isoformat()))
    where = "WHERE " + " AND ".join(conditions)
    return await query(f"""
        SELECT
          e.id,
          e.descrizione,
          e.data,
          e.ora_evento,
          e.stato,
          c.nome                                                       AS cliente,
          l.nome                                                       AS location_nome,
          COALESCE(e.n_adulti,0) + COALESCE(e.n_bambini,0)
            + COALESCE(e.n_fornitori,0) + COALESCE(e.n_altri,0)       AS tot_ospiti
        FROM {_table('eventi')} e
        LEFT JOIN {_table('location')} l ON l.id = e.id_location
        LEFT JOIN {_table('clienti')} c ON c.id = e.id_cliente
        {where}
        ORDER BY SAFE_CAST(e.data AS DATE)
    """, params)


@router.get("/consuntivo/{id_evento}")
async def consuntivo(id_evento: int):
    return await query(f"""
        SELECT
          p.id,
          p.cod_articolo,
          a.descrizione,
          p.qta,
          p.qta_ape,
          p.qta_sedu,
          p.qta_bufdol,
          p.note,
          c.cod_tipo,
          t.descrizione   AS tipo_desc,
          COALESCE(t.cod_step, 999) AS ordine
        FROM {_table('eventi_det_prel')} p
        LEFT JOIN {_table('articoli')} a ON a.cod_articolo = p.cod_articolo
        LEFT JOIN {_table('tb_codici_categ')} c ON c.cod_categ = a.cod_categ
        LEFT JOIN {_table('tb_tipi_mat')} t ON t.cod_tipo = c.cod_tipo
        WHERE p.id_evento = @id
        ORDER BY COALESCE(t.cod_step, 999), a.rank NULLS LAST, p.cod_articolo
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])


@router.get("/impegni-magazzino")
async def impegni_magazzino(
    data_da: date | None = None,
    data_a: date | None = None,
    cod_articolo: str | None = None,
):
    conditions = [
        "COALESCE(e.deleted, FALSE) = FALSE",
        "e.stato != 'annullato'",
        "SAFE_CAST(e.data AS DATE) >= CURRENT_DATE()",
    ]
    params: list = []
    if data_da:
        conditions.append("SAFE_CAST(e.data AS DATE) >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da.isoformat()))
    if data_a:
        conditions.append("SAFE_CAST(e.data AS DATE) <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a.isoformat()))
    if cod_articolo:
        conditions.append("p.cod_articolo = @cod")
        params.append(bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo))
    where = "WHERE " + " AND ".join(conditions)
    return await query(f"""
        SELECT
          SAFE_CAST(e.data AS DATE)                                    AS data,
          l.nome                                                       AS location,
          e.id                                                         AS id_evento,
          e.descrizione                                                AS evento,
          p.cod_articolo,
          a.descrizione,
          p.qta,
          a.qta_giac,
          c.cod_tipo
        FROM {_table('eventi_det_prel')} p
        JOIN {_table('eventi')} e ON e.id = p.id_evento
        LEFT JOIN {_table('articoli')} a ON a.cod_articolo = p.cod_articolo
        LEFT JOIN {_table('location')} l ON l.id = e.id_location
        LEFT JOIN {_table('tb_codici_categ')} c ON c.cod_categ = a.cod_categ
        {where}
        ORDER BY SAFE_CAST(e.data AS DATE), p.cod_articolo
    """, params)


@router.get("/acconti-in-scadenza")
async def acconti_in_scadenza():
    """Acconti con data_scadenza nei prossimi 65 giorni."""
    return await query(f"""
        SELECT
          e.id,
          e.descrizione,
          e.data                                                       AS data_evento,
          c.nome                                                       AS cliente,
          a.importo,
          a.data_scadenza,
          a.is_conferma,
          DATE_DIFF(SAFE_CAST(a.data_scadenza AS DATE), CURRENT_DATE(), DAY) AS giorni_alla_scadenza
        FROM {_table('evento_acconti')} a
        JOIN {_table('eventi')} e ON e.id = a.id_evento
        LEFT JOIN {_table('clienti')} c ON c.id = e.id_cliente
        WHERE COALESCE(e.deleted, FALSE) = FALSE
          AND e.stato NOT IN ('annullato', 'bozza')
          AND a.data_scadenza IS NOT NULL
          AND SAFE_CAST(a.data_scadenza AS DATE) >= CURRENT_DATE()
          AND SAFE_CAST(a.data_scadenza AS DATE) <= DATE_ADD(CURRENT_DATE(), INTERVAL 65 DAY)
        ORDER BY SAFE_CAST(a.data_scadenza AS DATE)
    """)


@router.get("/costi-per-tipo/{id_evento}")
async def costi_per_tipo(id_evento: int):
    """Costi per tipo ospite per un evento (unpivot dei campi n_*/costo_* di eventi)."""
    return await query(f"""
        SELECT ospiti.cod_tipo, tt.descrizione, ospiti.numero, ospiti.costo
        FROM (
          SELECT
            '8' AS cod_tipo, COALESCE(n_adulti, 0) AS numero, COALESCE(costo_adulti, 0) AS costo
          FROM {_table('eventi')} WHERE id = @id
          UNION ALL
          SELECT
            '5', COALESCE(n_bambini, 0), COALESCE(costo_bambini, 0)
          FROM {_table('eventi')} WHERE id = @id
          UNION ALL
          SELECT
            '7', COALESCE(n_fornitori, 0), COALESCE(costo_fornitori, 0)
          FROM {_table('eventi')} WHERE id = @id
          UNION ALL
          SELECT
            '6', COALESCE(n_altri, 0), COALESCE(costo_altri, 0)
          FROM {_table('eventi')} WHERE id = @id
        ) ospiti
        LEFT JOIN {_table('tb_tipi_ospiti')} tt ON tt.cod_tipo = ospiti.cod_tipo
        WHERE ospiti.numero > 0
        ORDER BY ospiti.cod_tipo
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
