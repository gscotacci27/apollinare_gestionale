"""Business logic: calcolo costi preventivo/consuntivo evento."""
from __future__ import annotations

from db.bigquery import _table, query
from google.cloud import bigquery


async def get_preventivo(id_evento: int) -> dict:
    """Ritorna il breakdown completo dei costi per un evento."""

    # Costo ospiti
    ospiti = await query(f"""
        SELECT numero, costo
        FROM {_table('GET_COSTO_OSPITI_EVT')}
        WHERE id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    # Costo articoli per tipo materiale
    tipi = await query(f"""
        SELECT t.cod_tipo, m.descrizione, t.numero, t.costo, t.costo_ivato
        FROM {_table('GET_COSTO_TIPI_EVT')} t
        JOIN {_table('TB_TIPI_MAT')} m ON m.cod_tipo = t.cod_tipo
        WHERE t.id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    # Costo risorse
    risorse = await query(f"""
        SELECT costo
        FROM {_table('GET_COSTO_RIS_EVT')}
        WHERE id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    # Costo degustazioni detraibili
    degust = await query(f"""
        SELECT costo
        FROM {_table('GET_COSTO_DEGUS_EVT')}
        WHERE id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    # Extra costi
    extra = await query(f"""
        SELECT SUM(costo * quantity) AS totale
        FROM {_table('EVENTI_ALTRICOSTI')}
        WHERE id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    costo_ospiti   = float(ospiti[0]["costo"] or 0)  if ospiti  else 0
    costo_risorse  = float(risorse[0]["costo"] or 0) if risorse else 0
    costo_degust   = float(degust[0]["costo"] or 0)  if degust  else 0
    costo_extra    = float(extra[0]["totale"] or 0)  if extra   else 0
    costo_articoli = sum(float(r["costo"] or 0) for r in tipi)
    costo_articoli_iva = sum(float(r["costo_ivato"] or 0) for r in tipi)

    totale_netto = costo_ospiti + costo_articoli + costo_risorse + costo_extra - costo_degust
    totale_ivato = costo_ospiti + costo_articoli_iva + costo_risorse + costo_extra - costo_degust

    return {
        "id_evento": id_evento,
        "ospiti": {
            "numero": int(ospiti[0]["numero"] or 0) if ospiti else 0,
            "costo": round(costo_ospiti, 2),
        },
        "articoli_per_tipo": [
            {
                "cod_tipo": r["cod_tipo"],
                "descrizione": r["descrizione"],
                "numero": r["numero"],
                "costo": round(float(r["costo"] or 0), 2),
                "costo_ivato": round(float(r["costo_ivato"] or 0), 2),
            }
            for r in tipi
        ],
        "risorse": round(costo_risorse, 2),
        "degustazioni_detraibili": round(costo_degust, 2),
        "extra": round(costo_extra, 2),
        "totale_netto": round(totale_netto, 2),
        "totale_ivato": round(totale_ivato, 2),
    }
