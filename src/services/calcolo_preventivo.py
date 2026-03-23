"""Business logic: calcolo costi preventivo/consuntivo evento.

Nota: il calcolo principale avviene in services/cache.py (calcola_preventivo,
puro Python su cache in memoria). Questo modulo fornisce un calcolo alternativo
direttamente da BQ, utile per report/export indipendenti dalla cache.
"""
from __future__ import annotations

from db.bigquery import _table, query
from google.cloud import bigquery


async def get_preventivo(id_evento: int) -> dict:
    """Ritorna il breakdown completo dei costi per un evento (da BQ diretto)."""
    param = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]

    # Costo ospiti (da colonne pivotate di eventi)
    evt_rows = await query(f"""
        SELECT
          COALESCE(n_adulti,0) * COALESCE(costo_adulti,0) * (1 - COALESCE(sconto_adulti,0)/100) +
          COALESCE(n_bambini,0) * COALESCE(costo_bambini,0) * (1 - COALESCE(sconto_bambini,0)/100) +
          COALESCE(n_fornitori,0) * COALESCE(costo_fornitori,0) * (1 - COALESCE(sconto_fornitori,0)/100) +
          COALESCE(n_altri,0) * COALESCE(costo_altri,0) * (1 - COALESCE(sconto_altri,0)/100)
            AS costo_ospiti,
          COALESCE(n_adulti,0) + COALESCE(n_bambini,0) +
          COALESCE(n_fornitori,0) + COALESCE(n_altri,0) AS tot_ospiti,
          COALESCE(sconto_totale, 0) AS sconto_totale,
          totale_manuale
        FROM {_table('eventi')}
        WHERE id = @id
        LIMIT 1
    """, param)

    # Costo articoli da lista di carico × prezzi_listino
    prel_rows = await query(f"""
        SELECT
          SUM((p.qta_ape + p.qta_sedu + p.qta_bufdol
               + p.qta_man_ape + p.qta_man_sedu + p.qta_man_bufdol)
              * COALESCE(pl.prezzo_netto, 0)) AS costo_articoli
        FROM {_table('eventi_det_prel')} p
        LEFT JOIN (
            SELECT cod_articolo, prezzo_netto
            FROM {_table('prezzi_listino')}
            WHERE prezzo_netto IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cod_articolo ORDER BY valido_dal DESC) = 1
        ) pl ON pl.cod_articolo = p.cod_articolo
        WHERE p.id_evento = @id
    """, param)

    # Costo extra
    extra_rows = await query(f"""
        SELECT COALESCE(SUM(costo * quantity), 0) AS totale
        FROM {_table('evento_extra')}
        WHERE id_evento = @id
    """, param)

    # Degustazioni detraibili
    degust_rows = await query(f"""
        SELECT COALESCE(SUM(costo), 0) AS totale
        FROM {_table('evento_degust')}
        WHERE id_evento = @id AND detraibile = TRUE
    """, param)

    # Acconti versati
    acconti_rows = await query(f"""
        SELECT COALESCE(SUM(importo), 0) AS totale
        FROM {_table('evento_acconti')}
        WHERE id_evento = @id
    """, param)

    evt = evt_rows[0] if evt_rows else {}
    costo_ospiti   = float(evt.get("costo_ospiti") or 0)
    costo_articoli = float(prel_rows[0]["costo_articoli"] or 0) if prel_rows else 0
    costo_extra    = float(extra_rows[0]["totale"] or 0) if extra_rows else 0
    costo_degust   = float(degust_rows[0]["totale"] or 0) if degust_rows else 0
    acconti_totale = float(acconti_rows[0]["totale"] or 0) if acconti_rows else 0
    sconto_totale  = float(evt.get("sconto_totale") or 0)
    totale_manuale = float(evt["totale_manuale"]) if evt.get("totale_manuale") is not None else None

    totale_calc = costo_ospiti + costo_articoli + costo_extra - costo_degust - sconto_totale
    totale_netto = totale_manuale if totale_manuale is not None else totale_calc

    return {
        "id_evento":               id_evento,
        "costo_ospiti":            round(costo_ospiti, 2),
        "costo_articoli":          round(costo_articoli, 2),
        "costo_extra":             round(costo_extra, 2),
        "degustazioni_detraibili": round(costo_degust, 2),
        "sconto_totale":           round(sconto_totale, 2),
        "totale_netto":            round(totale_netto, 2),
        "totale_manuale":          round(totale_manuale, 2) if totale_manuale is not None else None,
        "acconti_totale":          round(acconti_totale, 2),
        "saldo":                   round(totale_netto - acconti_totale, 2),
    }
