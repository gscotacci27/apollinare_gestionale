"""Business logic: calcolo automatico quantità lista di carico.

Replica la logica Oracle F_LIST_PRELIEVO_ADD_ARTICOLO.

flg_qta_type:
  'S' = Standard  → qta_std_a / qta_std_s / qta_std_b come valore fisso
  'C' = Coeff     → n_ospiti × coeff_a / coeff_s / coeff_b
  'P' = Perc      → totale_ospiti × perc_ospiti / 100
  None/altro      → 0
"""
from __future__ import annotations

from dataclasses import dataclass

from db.bigquery import _table, query
from google.cloud import bigquery


@dataclass
class OspitiCounts:
    adulti: float = 0
    aperitivo: float = 0
    seduto: float = 0
    buffet_dolci: float = 0

    @property
    def totale(self) -> float:
        return self.aperitivo + self.seduto + self.buffet_dolci


async def get_ospiti(id_evento: int) -> OspitiCounts:
    """Distribuzione ospiti per servizio dall'evento (tot_ospiti + perc_sedute_aper)."""
    rows = await query(f"""
        SELECT
          n_adulti,
          COALESCE(n_adulti,0) + COALESCE(n_bambini,0)
            + COALESCE(n_fornitori,0) + COALESCE(n_altri,0) AS tot_ospiti,
          perc_sedute_aper
        FROM {_table('eventi')}
        WHERE id = @id_evento
          AND COALESCE(deleted, FALSE) = FALSE
        LIMIT 1
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])

    if not rows:
        return OspitiCounts()

    e = rows[0]
    tot = float(e.get("tot_ospiti") or 0)
    adulti = float(e.get("n_adulti") or 0)
    perc_aper = float(e.get("perc_sedute_aper") or 0) / 100.0
    n_aper = round(tot * perc_aper) if perc_aper else 0
    n_sedu = tot - n_aper

    return OspitiCounts(adulti=adulti, aperitivo=n_aper, seduto=n_sedu, buffet_dolci=0)


def calcola_qta(articolo: dict, ospiti: OspitiCounts) -> dict[str, float]:
    """Calcola qta_ape, qta_sedu, qta_bufdol in base al flg_qta_type.

    Allineato alla documentazione ingestion_data:
    - S: quantità fisse da qta_std_*
    - C: coefficienti × ospiti per fase
    - P: percentuale sugli adulti
    """
    flg = (articolo.get("flg_qta_type") or "").upper()

    if flg == "S":
        return {
            "qta_ape": float(articolo.get("qta_std_a") or 0),
            "qta_sedu": float(articolo.get("qta_std_s") or 0),
            "qta_bufdol": float(articolo.get("qta_std_b") or 0),
        }

    if flg == "C":
        coeff_a = float(articolo.get("coeff_a") or 0)
        coeff_s = float(articolo.get("coeff_s") or 0)
        coeff_b = float(articolo.get("coeff_b") or 0)
        qta_a = ospiti.aperitivo * coeff_a
        qta_s = ospiti.seduto * coeff_s
        qta_b = ospiti.buffet_dolci * coeff_b
        return {"qta_ape": round(qta_a, 1), "qta_sedu": round(qta_s, 1), "qta_bufdol": round(qta_b, 1)}

    if flg == "P":
        perc = float(articolo.get("perc_ospiti") or 100) / 100.0
        totale = round(ospiti.adulti * perc)
        return {"qta_ape": totale, "qta_sedu": 0, "qta_bufdol": 0}

    return {"qta_ape": 0, "qta_sedu": 0, "qta_bufdol": 0}


async def fetch_articolo(cod_articolo: str) -> dict | None:
    rows = await query(f"""
        SELECT *
        FROM {_table('articoli')}
        WHERE cod_articolo = @cod
        LIMIT 1
    """, [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)])
    return rows[0] if rows else None


async def get_next_id_lista() -> int:
    """ID globale univoco per eventi_det_prel (MAX globale, non per-evento)."""
    rows = await query(
        f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {_table('eventi_det_prel')}"
    )
    return int(rows[0]["next_id"])


async def get_next_ordine(id_evento: int) -> int:
    rows = await query(f"""
        SELECT COALESCE(MAX(ordine), 0) + 10 AS next_ordine
        FROM {_table('eventi_det_prel')}
        WHERE id_evento = @id_evento
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])
    return int(rows[0]["next_ordine"])
