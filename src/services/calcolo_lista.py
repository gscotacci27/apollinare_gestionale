"""Business logic: calcolo automatico quantità lista di carico.

Replica la logica Oracle F_LIST_PRELIEVO_ADD_ARTICOLO.

FLG_QTA_TYPE:
  'S' = Standard  → QTA_STD_A / QTA_STD_S / QTA_STD_B come valore fisso
  'C' = Coeff     → n_ospiti × COEFF_A / COEFF_S / COEFF_B
  'P' = Perc      → totale_ospiti × PERC_OSPITI / 100
  None/altro      → 0
"""
from __future__ import annotations

from dataclasses import dataclass

from db.bigquery import _table, query
from google.cloud import bigquery


@dataclass
class OspitiCounts:
    aperitivo: float = 0
    seduto: float = 0
    buffet_dolci: float = 0

    @property
    def totale(self) -> float:
        return self.aperitivo + self.seduto + self.buffet_dolci


async def get_ospiti(id_evento: int) -> OspitiCounts:
    """Distribuzione ospiti per servizio dall'evento (TOT_OSPITI + PERC_SEDUTE_APER)."""
    rows = await query(f"""
        WITH dedup AS (
            SELECT
                CAST(TOT_OSPITI AS INT64)  AS tot_ospiti,
                PERC_SEDUTE_APER
            FROM {_table('EVENTI')}
            WHERE CAST(ID AS INT64) = @id_evento
              AND (DELETED IS NULL OR CAST(DELETED AS INT64) = 0)
              AND (IS_TEMPLATE IS NULL OR CAST(IS_TEMPLATE AS INT64) = 0)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)
            ) = 1
        )
        SELECT * FROM dedup
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])

    if not rows:
        return OspitiCounts()

    e = rows[0]
    tot = float(e.get("tot_ospiti") or 0)
    perc_aper = float(e.get("perc_sedute_aper") or 0) / 100.0
    n_aper = round(tot * perc_aper) if perc_aper else 0
    n_sedu = tot - n_aper

    return OspitiCounts(aperitivo=n_aper, seduto=n_sedu, buffet_dolci=0)


def calcola_qta(articolo: dict, ospiti: OspitiCounts) -> dict[str, float]:
    """Calcola QTA_APE, QTA_SEDU, QTA_BUFDOL in base al FLG_QTA_TYPE.

    Nota: il DB Oracle mostra che sia 'S' che 'C' usano COEFF × ospiti.
    La differenza tra 'S' e 'C' non è nel calcolo ma nell'origine del coefficiente
    (standard vs configurato). Entrambi usano COEFF_A/S/B × n_ospiti.
    Se il coefficiente è 0 e QTA_STD è impostato si usa QTA_STD come fallback.
    """
    flg = (articolo.get("FLG_QTA_TYPE") or "S").upper()

    if flg in ("S", "C"):
        coeff_a = float(articolo.get("COEFF_A") or 0)
        coeff_s = float(articolo.get("COEFF_S") or 0)
        coeff_b = float(articolo.get("COEFF_B") or 0)
        # Fallback a QTA_STD solo se COEFF è zero
        qta_a = ospiti.aperitivo    * coeff_a if coeff_a else float(articolo.get("QTA_STD_A") or 0)
        qta_s = ospiti.seduto       * coeff_s if coeff_s else float(articolo.get("QTA_STD_S") or 0)
        qta_b = ospiti.buffet_dolci * coeff_b if coeff_b else float(articolo.get("QTA_STD_B") or 0)
        return {"qta_ape": round(qta_a, 1), "qta_sedu": round(qta_s, 1), "qta_bufdol": round(qta_b, 1)}

    if flg == "P":
        perc = float(articolo.get("PERC_OSPITI") or 100) / 100.0
        totale = round(ospiti.totale * perc)
        return {"qta_ape": totale, "qta_sedu": 0, "qta_bufdol": 0}

    return {"qta_ape": 0, "qta_sedu": 0, "qta_bufdol": 0}


async def fetch_articolo(cod_articolo: str) -> dict | None:
    rows = await query(f"""
        SELECT *
        FROM {_table('ARTICOLI')}
        WHERE COD_ARTICOLO = @cod
        QUALIFY ROW_NUMBER() OVER (PARTITION BY COD_ARTICOLO ORDER BY COD_ARTICOLO) = 1
    """, [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)])
    return rows[0] if rows else None


async def get_next_id_lista() -> int:
    """ID globale univoco per EVENTI_DET_PREL (MAX globale, non per-evento)."""
    rows = await query(
        f"SELECT COALESCE(MAX(CAST(ID AS INT64)), 0) + 1 AS next_id FROM {_table('EVENTI_DET_PREL')}"
    )
    return int(rows[0]["next_id"])


async def get_next_ordine(id_evento: int) -> int:
    rows = await query(f"""
        SELECT COALESCE(MAX(CAST(ORDINE AS INT64)), 0) + 10 AS next_ordine
        FROM {_table('EVENTI_DET_PREL')}
        WHERE CAST(ID_EVENTO AS INT64) = @id_evento
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])
    return int(rows[0]["next_ordine"])
