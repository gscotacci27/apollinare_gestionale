"""Business logic: calcolo automatico quantità lista di carico.

Replica la logica Oracle F_LIST_PRELIEVO_ADD_ARTICOLO.

FLG_QTA_TYPE:
  'S' = Standard  → usa QTA_STD_A / QTA_STD_S / QTA_STD_B come valore fisso
  'C' = Coeff     → n_ospiti × COEFF_A / COEFF_S / COEFF_B
  'P' = Perc      → n_ospiti × PERC_OSPITI / 100
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
    """Recupera i conteggi ospiti per tipo di servizio."""
    # cod_tipo_ospite: 8=adulti, altri codici per bambini/neonati
    # usiamo il totale ospiti adulti per il calcolo
    rows = await query(f"""
        SELECT cod_tipo_ospite, numero
        FROM {_table('EVENTI_DET_OSPITI')}
        WHERE id_evento = @id_evento
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])

    # Per semplicità: tipo 8 = adulti (base per il calcolo)
    adulti = sum(r["numero"] or 0 for r in rows if str(r["cod_tipo_ospite"]) == "8")

    # Leggiamo il tipo di servizio dall'evento per distribuzione ape/sedu/buf
    evt = await query(f"""
        SELECT gran_buffet_a, servizio_tavolo_a, buffet_dolci_a,
               gran_buffet_b, servizio_tavolo_b, buffet_dolci_b,
               perc_sedute_aper
        FROM {_table('EVENTI')}
        WHERE id = @id_evento
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])

    if not evt:
        return OspitiCounts(aperitivo=adulti)

    e = evt[0]
    perc_aper = float(e.get("perc_sedute_aper") or 0) / 100.0
    n_aper = round(adulti * perc_aper) if perc_aper else 0
    n_sedu = adulti - n_aper

    return OspitiCounts(
        aperitivo=n_aper,
        seduto=n_sedu,
        buffet_dolci=0,  # gestito separatamente dal buffet dolci
    )


def calcola_qta(
    articolo: dict,
    ospiti: OspitiCounts,
) -> dict[str, float]:
    """Calcola QTA_APE, QTA_SEDU, QTA_BUFDOL in base al tipo di calcolo."""
    flg = (articolo.get("FLG_QTA_TYPE") or "S").upper()

    if flg == "S":
        # Quantità fisse standard
        return {
            "qta_ape":    float(articolo.get("QTA_STD_A") or 0),
            "qta_sedu":   float(articolo.get("QTA_STD_S") or 0),
            "qta_bufdol": float(articolo.get("QTA_STD_B") or 0),
        }

    if flg == "C":
        # Numero ospiti × coefficiente
        return {
            "qta_ape":    ospiti.aperitivo    * float(articolo.get("COEFF_A") or 1),
            "qta_sedu":   ospiti.seduto       * float(articolo.get("COEFF_S") or 1),
            "qta_bufdol": ospiti.buffet_dolci * float(articolo.get("COEFF_B") or 1),
        }

    if flg == "P":
        # Percentuale sul totale ospiti
        perc = float(articolo.get("PERC_OSPITI") or 100) / 100.0
        totale = round(ospiti.totale * perc)
        return {
            "qta_ape":    totale,
            "qta_sedu":   0,
            "qta_bufdol": 0,
        }

    return {"qta_ape": 0, "qta_sedu": 0, "qta_bufdol": 0}


async def fetch_articolo(cod_articolo: str) -> dict | None:
    rows = await query(f"""
        SELECT *
        FROM {_table('ARTICOLI')}
        WHERE cod_articolo = @cod
    """, [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)])
    return rows[0] if rows else None


async def get_next_id_lista(id_evento: int) -> int:
    rows = await query(f"""
        SELECT COALESCE(MAX(id), 0) + 1 AS next_id
        FROM {_table('EVENTI_DET_PREL')}
        WHERE id_evento = @id_evento
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])
    return int(rows[0]["next_id"])


async def get_next_ordine(id_evento: int) -> int:
    rows = await query(f"""
        SELECT COALESCE(MAX(ordine), 0) + 10 AS next_ordine
        FROM {_table('EVENTI_DET_PREL')}
        WHERE id_evento = @id_evento
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])
    return int(rows[0]["next_ordine"])
