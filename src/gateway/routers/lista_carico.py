"""Router: lista di carico (EVENTI_DET_PREL) con calcolo automatico quantità."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery

from db.bigquery import _table, dml, insert, query
from models.articolo import AddArticoloRequest
from models.evento import ListaCaricaItem
from services.calcolo_lista import (
    calcola_qta,
    fetch_articolo,
    get_next_id_lista,
    get_next_ordine,
    get_ospiti,
)

router = APIRouter(prefix="/eventi/{id_evento}/lista", tags=["lista-carico"])


@router.get("", response_model=list[ListaCaricaItem])
async def get_lista(id_evento: int):
    rows = await query(f"""
        SELECT p.*, a.descrizione
        FROM {_table('EVENTI_DET_PREL')} p
        LEFT JOIN {_table('ARTICOLI')} a ON a.cod_articolo = p.cod_articolo
        WHERE p.id_evento = @id
        ORDER BY p.ordine
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
    return [ListaCaricaItem(**{k.lower(): v for k, v in r.items()}) for r in rows]


@router.post("", response_model=dict, status_code=201)
async def add_articolo(id_evento: int, body: AddArticoloRequest):
    """Aggiunge un articolo alla lista con calcolo automatico delle quantità."""
    articolo = await fetch_articolo(body.cod_articolo)
    if not articolo:
        raise HTTPException(404, f"Articolo {body.cod_articolo!r} non trovato")

    ospiti = await get_ospiti(id_evento)
    qtadict = calcola_qta(articolo, ospiti)

    new_id = await get_next_id_lista(id_evento)
    ordine = await get_next_ordine(id_evento)

    row = {
        "ID_EVENTO": id_evento,
        "ID": new_id,
        "COD_ARTICOLO": body.cod_articolo,
        "QTA": 0,
        "QTA_APE":     qtadict["qta_ape"],
        "QTA_SEDU":    qtadict["qta_sedu"],
        "QTA_BUFDOL":  qtadict["qta_bufdol"],
        "QTA_MAN_APE":   body.qta_man_ape,
        "QTA_MAN_SEDU":  body.qta_man_sedu,
        "QTA_MAN_BUFDOL": body.qta_man_bufdol,
        "NOTE": body.note,
        "ORDINE": ordine,
    }
    await insert("EVENTI_DET_PREL", row)
    return {
        "id": new_id,
        "cod_articolo": body.cod_articolo,
        **qtadict,
    }


@router.put("/{item_id}", response_model=dict)
async def update_articolo(id_evento: int, item_id: int, body: AddArticoloRequest):
    """Aggiorna quantità manuali di una riga."""
    affected = await dml(f"""
        UPDATE {_table('EVENTI_DET_PREL')}
        SET qta_man_ape = @qma, qta_man_sedu = @qms, qta_man_bufdol = @qmb,
            note = @note
        WHERE id_evento = @id_evento AND id = @item_id
    """, [
        bigquery.ScalarQueryParameter("qma",      "FLOAT64", body.qta_man_ape),
        bigquery.ScalarQueryParameter("qms",      "FLOAT64", body.qta_man_sedu),
        bigquery.ScalarQueryParameter("qmb",      "FLOAT64", body.qta_man_bufdol),
        bigquery.ScalarQueryParameter("note",     "STRING",  body.note or ""),
        bigquery.ScalarQueryParameter("id_evento","INT64",   id_evento),
        bigquery.ScalarQueryParameter("item_id",  "INT64",   item_id),
    ])
    if affected == 0:
        raise HTTPException(404, "Riga lista non trovata")
    return {"updated": item_id}


@router.delete("/{item_id}", response_model=dict)
async def remove_articolo(id_evento: int, item_id: int):
    affected = await dml(f"""
        DELETE FROM {_table('EVENTI_DET_PREL')}
        WHERE id_evento = @id_evento AND id = @item_id
    """, [
        bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento),
        bigquery.ScalarQueryParameter("item_id",   "INT64", item_id),
    ])
    if affected == 0:
        raise HTTPException(404, "Riga lista non trovata")
    return {"deleted": item_id}


@router.post("/ricalcola", response_model=dict)
async def ricalcola_lista(id_evento: int):
    """Ricalcola le quantità automatiche per tutti gli articoli della lista."""
    ospiti = await get_ospiti(id_evento)
    rows = await query(f"""
        SELECT p.id, p.cod_articolo
        FROM {_table('EVENTI_DET_PREL')} p
        WHERE p.id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    updated = 0
    for r in rows:
        articolo = await fetch_articolo(r["cod_articolo"])
        if not articolo:
            continue
        qtadict = calcola_qta(articolo, ospiti)
        await dml(f"""
            UPDATE {_table('EVENTI_DET_PREL')}
            SET qta_ape = @qa, qta_sedu = @qs, qta_bufdol = @qb
            WHERE id_evento = @id_evento AND id = @item_id
        """, [
            bigquery.ScalarQueryParameter("qa",       "FLOAT64", qtadict["qta_ape"]),
            bigquery.ScalarQueryParameter("qs",       "FLOAT64", qtadict["qta_sedu"]),
            bigquery.ScalarQueryParameter("qb",       "FLOAT64", qtadict["qta_bufdol"]),
            bigquery.ScalarQueryParameter("id_evento","INT64",   id_evento),
            bigquery.ScalarQueryParameter("item_id",  "INT64",   r["id"]),
        ])
        updated += 1

    return {"ricalcolati": updated}
