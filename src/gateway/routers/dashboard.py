"""Router dashboard — widget aggregati per la home page."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter
from google.cloud import bigquery

from db.bigquery import _table, query

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


# ── KPI ────────────────────────────────────────────────────────────────────────

@router.get("/kpi")
async def kpi() -> dict:
    ev_q = query(f"""
        SELECT COUNT(*) AS cnt
        FROM {_table('eventi')}
        WHERE COALESCE(deleted, FALSE) = FALSE
          AND stato != 'annullato'
          AND SAFE_CAST(data AS DATE) >= CURRENT_DATE()
    """)
    liste_q = query(f"""
        SELECT COUNT(DISTINCT p.id_evento) AS cnt
        FROM {_table('eventi_det_prel')} p
        JOIN {_table('eventi')} e ON e.id = p.id_evento
        WHERE COALESCE(e.deleted, FALSE) = FALSE
          AND e.stato != 'annullato'
          AND SAFE_CAST(e.data AS DATE) >= CURRENT_DATE()
    """)
    art_q = query(f"SELECT COUNT(*) AS cnt FROM {_table('articoli')}")
    r_ev, r_liste, r_art = await asyncio.gather(ev_q, liste_q, art_q)
    return {
        "eventi_attivi":   int(r_ev[0]["cnt"] or 0),
        "liste_aperte":    int(r_liste[0]["cnt"] or 0),
        "articoli_totali": int(r_art[0]["cnt"] or 0),
    }


# ── PROSSIMI EVENTI ────────────────────────────────────────────────────────────

@router.get("/prossimi-eventi")
async def prossimi_eventi() -> list[dict]:
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
        WHERE COALESCE(e.deleted, FALSE) = FALSE
          AND e.stato != 'annullato'
          AND SAFE_CAST(e.data AS DATE) >= CURRENT_DATE()
        ORDER BY SAFE_CAST(e.data AS DATE) ASC
        LIMIT 5
    """)


# ── LISTE APERTE ───────────────────────────────────────────────────────────────

@router.get("/liste-aperte")
async def liste_aperte() -> list[dict]:
    return await query(f"""
        WITH eventi_con_lista AS (
          SELECT DISTINCT id_evento
          FROM {_table('eventi_det_prel')}
        )
        SELECT
          e.id,
          e.descrizione,
          e.data,
          e.stato,
          c.nome                                                       AS cliente,
          l.nome                                                       AS location_nome,
          COALESCE(e.n_adulti,0) + COALESCE(e.n_bambini,0)
            + COALESCE(e.n_fornitori,0) + COALESCE(e.n_altri,0)       AS tot_ospiti
        FROM {_table('eventi')} e
        JOIN eventi_con_lista ecl ON ecl.id_evento = e.id
        LEFT JOIN {_table('location')} l ON l.id = e.id_location
        LEFT JOIN {_table('clienti')} c ON c.id = e.id_cliente
        WHERE COALESCE(e.deleted, FALSE) = FALSE
          AND e.stato != 'annullato'
          AND SAFE_CAST(e.data AS DATE) >= CURRENT_DATE()
        ORDER BY SAFE_CAST(e.data AS DATE) ASC
        LIMIT 5
    """)


# ── CARICO DI LAVORO ───────────────────────────────────────────────────────────

@router.get("/carico-lavoro")
async def carico_lavoro() -> list[dict]:
    rows = await query(f"""
        SELECT
          FORMAT_DATE('%G-W%V', SAFE_CAST(data AS DATE)) AS settimana,
          CASE stato
            WHEN 'in_attesa_conferma' THEN 'preventivo'
            WHEN 'in_lavorazione'     THEN 'in_lavorazione'
            WHEN 'confermato'         THEN 'confermato'
            ELSE 'altro'
          END AS stato_gruppo,
          COUNT(*) AS cnt
        FROM {_table('eventi')}
        WHERE COALESCE(deleted, FALSE) = FALSE
          AND stato != 'annullato'
          AND SAFE_CAST(data AS DATE) >= CURRENT_DATE()
          AND SAFE_CAST(data AS DATE) < DATE_ADD(CURRENT_DATE(), INTERVAL 8 WEEK)
        GROUP BY settimana, stato_gruppo
        ORDER BY settimana
    """)

    settimane: dict[str, dict] = {}
    for r in rows:
        s = r["settimana"]
        if s not in settimane:
            settimane[s] = {"settimana": s, "preventivo": 0, "in_lavorazione": 0, "confermato": 0}
        sg = r["stato_gruppo"]
        if sg in settimane[s]:
            settimane[s][sg] += int(r["cnt"] or 0)
    return list(settimane.values())


# ── ARTICOLI SOTTO SCORTA ──────────────────────────────────────────────────────

@router.get("/articoli-sotto-scorta")
async def articoli_sotto_scorta(
    giorni: int | None = None,
    data: str | None = None,
) -> list[dict]:
    if data:
        date_filter = "SAFE_CAST(e.data AS DATE) = @data_filter"
        params = [bigquery.ScalarQueryParameter("data_filter", "DATE", data)]
    else:
        n = giorni if giorni and giorni > 0 else 30
        date_filter = (
            "SAFE_CAST(e.data AS DATE) >= CURRENT_DATE() "
            "AND SAFE_CAST(e.data AS DATE) < DATE_ADD(CURRENT_DATE(), INTERVAL @giorni DAY)"
        )
        params = [bigquery.ScalarQueryParameter("giorni", "INT64", n)]

    return await query(f"""
        WITH impegni AS (
          SELECT p.cod_articolo, SUM(p.qta) AS qta_impegnata
          FROM {_table('eventi_det_prel')} p
          JOIN {_table('eventi')} e ON e.id = p.id_evento
          WHERE COALESCE(e.deleted, FALSE) = FALSE
            AND e.stato != 'annullato'
            AND {date_filter}
          GROUP BY p.cod_articolo
        )
        SELECT
          a.cod_articolo,
          a.descrizione,
          a.qta_giac,
          COALESCE(i.qta_impegnata, 0)                                           AS qta_impegnata,
          ROUND(COALESCE(i.qta_impegnata, 0) / a.qta_giac * 100, 1)             AS perc_impegnata
        FROM {_table('articoli')} a
        LEFT JOIN impegni i ON i.cod_articolo = a.cod_articolo
        WHERE a.qta_giac > 0
          AND a.qta_giac != 9999
          AND COALESCE(i.qta_impegnata, 0) / a.qta_giac > 0.5
        ORDER BY perc_impegnata DESC
        LIMIT 20
    """, params)


# ── ATTIVITÀ RECENTI ───────────────────────────────────────────────────────────

@router.get("/attivita-recenti")
async def attivita_recenti() -> list[dict]:
    ev_q = query(f"""
        SELECT
          e.id,
          'evento'    AS tipo,
          e.descrizione,
          c.nome      AS cliente,
          e.data,
          e.stato
        FROM {_table('eventi')} e
        LEFT JOIN {_table('clienti')} c ON c.id = e.id_cliente
        WHERE COALESCE(e.deleted, FALSE) = FALSE
        ORDER BY e.id DESC
        LIMIT 6
    """)
    acc_q = query(f"""
        SELECT
          a.id,
          'acconto'           AS tipo,
          a.id_evento,
          a.importo,
          a.data_scadenza     AS data,
          e.descrizione,
          c.nome              AS cliente
        FROM {_table('evento_acconti')} a
        JOIN {_table('eventi')} e ON e.id = a.id_evento
        LEFT JOIN {_table('clienti')} c ON c.id = e.id_cliente
        WHERE COALESCE(e.deleted, FALSE) = FALSE
        ORDER BY a.id DESC
        LIMIT 4
    """)
    r_ev, r_acc = await asyncio.gather(ev_q, acc_q)

    combined: list[dict] = []
    for r in r_ev:
        combined.append({"tipo": "evento", **r})
    for r in r_acc:
        combined.append({"tipo": "acconto", **r})

    combined.sort(key=lambda x: x.get("data") or "", reverse=True)
    return combined[:10]
