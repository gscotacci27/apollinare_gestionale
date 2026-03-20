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
    """Contatori: eventi attivi, liste di carico attive, totale articoli."""
    ev_q = query(f"""
        SELECT COUNT(*) AS cnt
        FROM {_table('EVENTI')}
        WHERE COALESCE(CAST(DELETED AS INT64), 0) = 0
          AND COALESCE(CAST(IS_TEMPLATE AS INT64), 0) = 0
          AND CAST(STATO AS INT64) != 900
          AND SAFE_CAST(SUBSTR(DATA, 1, 10) AS DATE) >= CURRENT_DATE()
    """)
    liste_q = query(f"""
        SELECT COUNT(DISTINCT CAST(p.ID_EVENTO AS INT64)) AS cnt
        FROM {_table('EVENTI_DET_PREL')} p
        JOIN {_table('EVENTI')} e ON CAST(e.ID AS INT64) = CAST(p.ID_EVENTO AS INT64)
        WHERE COALESCE(CAST(e.DELETED AS INT64), 0) = 0
          AND CAST(e.STATO AS INT64) != 900
          AND SAFE_CAST(SUBSTR(e.DATA, 1, 10) AS DATE) >= CURRENT_DATE()
    """)
    art_q = query(f"""
        SELECT COUNT(*) AS cnt
        FROM {_table('ARTICOLI')}
    """)
    r_ev, r_liste, r_art = await asyncio.gather(ev_q, liste_q, art_q)
    return {
        "eventi_attivi": int(r_ev[0]["cnt"] or 0),
        "liste_aperte":  int(r_liste[0]["cnt"] or 0),
        "articoli_totali": int(r_art[0]["cnt"] or 0),
    }


# ── PROSSIMI EVENTI ────────────────────────────────────────────────────────────

@router.get("/prossimi-eventi")
async def prossimi_eventi() -> list[dict]:
    """Prossimi 5 eventi futuri, ordinati per data."""
    rows = await query(f"""
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
          CAST(e.ID AS INT64)          AS id,
          e.DESCRIZIONE                AS descrizione,
          SUBSTR(e.DATA, 1, 10)        AS data,
          e.ORA_EVENTO                 AS ora_evento,
          CAST(e.STATO AS INT64)       AS stato,
          e.CLIENTE                    AS cliente,
          l.location                   AS location_nome,
          CAST(e.TOT_OSPITI AS INT64)  AS tot_ospiti
        FROM dedup e
        LEFT JOIN loc_dedup l ON l.id = CAST(e.ID_LOCATION AS INT64)
        WHERE COALESCE(CAST(e.DELETED AS INT64), 0) = 0
          AND COALESCE(CAST(e.IS_TEMPLATE AS INT64), 0) = 0
          AND CAST(e.STATO AS INT64) != 900
          AND SAFE_CAST(SUBSTR(e.DATA, 1, 10) AS DATE) >= CURRENT_DATE()
        ORDER BY SAFE_CAST(SUBSTR(e.DATA, 1, 10) AS DATE) ASC
        LIMIT 5
    """)
    return rows


# ── LISTE APERTE ───────────────────────────────────────────────────────────────

@router.get("/liste-aperte")
async def liste_aperte() -> list[dict]:
    """Ultimi 5 eventi con lista di carico attiva (stato confermato, data >= oggi)."""
    rows = await query(f"""
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
        ),
        eventi_con_lista AS (
          SELECT DISTINCT CAST(ID_EVENTO AS INT64) AS id_evento
          FROM {_table('EVENTI_DET_PREL')}
        )
        SELECT
          CAST(e.ID AS INT64)          AS id,
          e.DESCRIZIONE                AS descrizione,
          SUBSTR(e.DATA, 1, 10)        AS data,
          CAST(e.STATO AS INT64)       AS stato,
          e.CLIENTE                    AS cliente,
          l.location                   AS location_nome,
          CAST(e.TOT_OSPITI AS INT64)  AS tot_ospiti
        FROM dedup e
        JOIN eventi_con_lista ecl ON ecl.id_evento = CAST(e.ID AS INT64)
        LEFT JOIN loc_dedup l ON l.id = CAST(e.ID_LOCATION AS INT64)
        WHERE COALESCE(CAST(e.DELETED AS INT64), 0) = 0
          AND COALESCE(CAST(e.IS_TEMPLATE AS INT64), 0) = 0
          AND CAST(e.STATO AS INT64) != 900
          AND SAFE_CAST(SUBSTR(e.DATA, 1, 10) AS DATE) >= CURRENT_DATE()
        ORDER BY SAFE_CAST(SUBSTR(e.DATA, 1, 10) AS DATE) ASC
        LIMIT 5
    """)
    return rows


# ── CARICO DI LAVORO ───────────────────────────────────────────────────────────

@router.get("/carico-lavoro")
async def carico_lavoro() -> list[dict]:
    """Conteggio eventi per settimana (prossime 8 settimane), raggruppati per stato."""
    rows = await query(f"""
        SELECT
          FORMAT_DATE('%G-W%V', SAFE_CAST(SUBSTR(DATA, 1, 10) AS DATE)) AS settimana,
          CASE
            WHEN CAST(STATO AS INT64) = 100 THEN 'preventivo'
            WHEN CAST(STATO AS INT64) IN (200, 300, 350) THEN 'in_lavorazione'
            WHEN CAST(STATO AS INT64) = 400 THEN 'confermato'
            ELSE 'altro'
          END AS stato_gruppo,
          COUNT(*) AS cnt
        FROM {_table('EVENTI')}
        WHERE COALESCE(CAST(DELETED AS INT64), 0) = 0
          AND COALESCE(CAST(IS_TEMPLATE AS INT64), 0) = 0
          AND CAST(STATO AS INT64) != 900
          AND SAFE_CAST(SUBSTR(DATA, 1, 10) AS DATE) >= CURRENT_DATE()
          AND SAFE_CAST(SUBSTR(DATA, 1, 10) AS DATE) < DATE_ADD(CURRENT_DATE(), INTERVAL 8 WEEK)
        GROUP BY settimana, stato_gruppo
        ORDER BY settimana
    """)

    # Pivot: settimana → { preventivo, in_lavorazione, confermato }
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
async def articoli_sotto_scorta() -> list[dict]:
    """Articoli impegnati >50% della giacenza (esclusi illimitati QTA_GIAC=9999)."""
    rows = await query(f"""
        WITH impegni AS (
          SELECT
            p.COD_ARTICOLO,
            SUM(CAST(p.QTA AS FLOAT64)) AS qta_impegnata
          FROM {_table('EVENTI_DET_PREL')} p
          JOIN {_table('EVENTI')} e ON CAST(e.ID AS INT64) = CAST(p.ID_EVENTO AS INT64)
          WHERE COALESCE(CAST(e.DELETED AS INT64), 0) = 0
            AND CAST(e.STATO AS INT64) != 900
            AND SAFE_CAST(SUBSTR(e.DATA, 1, 10) AS DATE) >= CURRENT_DATE()
          GROUP BY p.COD_ARTICOLO
        )
        SELECT
          a.COD_ARTICOLO      AS cod_articolo,
          a.DESCRIZIONE       AS descrizione,
          CAST(a.QTA_GIAC AS FLOAT64)  AS qta_giac,
          COALESCE(i.qta_impegnata, 0) AS qta_impegnata,
          ROUND(COALESCE(i.qta_impegnata, 0) / CAST(a.QTA_GIAC AS FLOAT64) * 100, 1) AS perc_impegnata
        FROM {_table('ARTICOLI')} a
        LEFT JOIN impegni i ON i.COD_ARTICOLO = a.COD_ARTICOLO
        WHERE CAST(a.QTA_GIAC AS FLOAT64) > 0
          AND CAST(a.QTA_GIAC AS FLOAT64) != 9999
          AND COALESCE(i.qta_impegnata, 0) / CAST(a.QTA_GIAC AS FLOAT64) > 0.5
        ORDER BY perc_impegnata DESC
        LIMIT 20
    """)
    return rows


# ── ATTIVITÀ RECENTI ───────────────────────────────────────────────────────────

@router.get("/attivita-recenti")
async def attivita_recenti() -> list[dict]:
    """Ultime attività: nuovi eventi + acconti recenti, ordinati per data desc."""
    ev_q = query(f"""
        WITH dedup AS (
          SELECT *
          FROM {_table('EVENTI')}
          QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)) = 1
        )
        SELECT
          CAST(ID AS INT64)  AS id,
          'evento'           AS tipo,
          DESCRIZIONE        AS descrizione,
          CLIENTE            AS cliente,
          SUBSTR(DATA, 1, 10) AS data,
          CAST(STATO AS INT64) AS stato
        FROM dedup
        WHERE COALESCE(CAST(DELETED AS INT64), 0) = 0
          AND COALESCE(CAST(IS_TEMPLATE AS INT64), 0) = 0
        ORDER BY CAST(ID AS INT64) DESC
        LIMIT 6
    """)
    acc_q = query(f"""
        SELECT
          CAST(a.ID AS INT64)         AS id,
          'acconto'                   AS tipo,
          CAST(a.ID_EVENTO AS INT64)  AS id_evento,
          CAST(a.ACCONTO AS FLOAT64)  AS importo,
          SUBSTR(CAST(a.DATA AS STRING), 1, 10) AS data,
          e.DESCRIZIONE               AS descrizione,
          e.CLIENTE                   AS cliente
        FROM {_table('EVENTI_ACCONTI')} a
        JOIN {_table('EVENTI')} e ON CAST(e.ID AS INT64) = CAST(a.ID_EVENTO AS INT64)
        WHERE COALESCE(CAST(e.DELETED AS INT64), 0) = 0
        ORDER BY CAST(a.ID AS INT64) DESC
        LIMIT 4
    """)
    r_ev, r_acc = await asyncio.gather(ev_q, acc_q)

    combined: list[dict] = []
    for r in r_ev:
        combined.append({"tipo": "evento", **r})
    for r in r_acc:
        combined.append({"tipo": "acconto", **r})

    # Sort by data desc (date string lexicographic is fine for ISO dates)
    combined.sort(key=lambda x: x.get("data") or "", reverse=True)
    return combined[:10]
