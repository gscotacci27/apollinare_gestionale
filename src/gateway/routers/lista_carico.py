"""Router: lista di carico (EVENTI_DET_PREL) — SF-002."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery

from db.bigquery import _table, dml, insert, query
from models.articolo import AddArticoloRequest, UpdateListaItemRequest
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
async def get_lista(id_evento: int) -> list[ListaCaricaItem]:
    rows = await query(f"""
        WITH dedup AS (
            SELECT *
            FROM {_table('EVENTI_DET_PREL')}
            WHERE CAST(ID_EVENTO AS INT64) = @id_evento
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(ID AS INT64) ORDER BY CAST(ID AS INT64)
            ) = 1
        ),
        art_dedup AS (
            SELECT COD_ARTICOLO,
                   ANY_VALUE(DESCRIZIONE) AS DESCRIZIONE,
                   ANY_VALUE(COD_CATEG)   AS COD_CATEG
            FROM {_table('ARTICOLI')}
            GROUP BY COD_ARTICOLO
        ),
        categ_dedup AS (
            SELECT COD_CATEG, ANY_VALUE(COD_TIPO) AS COD_TIPO
            FROM {_table('TB_CODICI_CATEG')}
            GROUP BY COD_CATEG
        ),
        tipo_dedup AS (
            SELECT COD_TIPO,
                   ANY_VALUE(DESCRIZIONE)              AS DESCRIZIONE,
                   ANY_VALUE(CAST(COD_STEP AS INT64))  AS COD_STEP
            FROM {_table('TB_TIPI_MAT')}
            GROUP BY COD_TIPO
        )
        SELECT
            CAST(d.ID AS INT64)                              AS id,
            d.COD_ARTICOLO                                   AS cod_articolo,
            a.DESCRIZIONE                                    AS descrizione,
            COALESCE(CAST(d.QTA AS FLOAT64), 0)              AS qta,
            COALESCE(CAST(d.QTA_APE AS FLOAT64), 0)          AS qta_ape,
            COALESCE(CAST(d.QTA_SEDU AS FLOAT64), 0)         AS qta_sedu,
            COALESCE(CAST(d.QTA_BUFDOL AS FLOAT64), 0)       AS qta_bufdol,
            COALESCE(CAST(d.QTA_MAN_APE AS FLOAT64), 0)      AS qta_man_ape,
            COALESCE(CAST(d.QTA_MAN_SEDU AS FLOAT64), 0)     AS qta_man_sedu,
            COALESCE(CAST(d.QTA_MAN_BUFDOL AS FLOAT64), 0)   AS qta_man_bufdol,
            d.NOTE                                           AS note,
            CAST(COALESCE(d.ORDINE, 0) AS INT64)             AS ordine,
            c.COD_TIPO                                       AS cod_tipo,
            t.DESCRIZIONE                                    AS tipo_descrizione,
            COALESCE(t.COD_STEP, 999)                        AS cod_step
        FROM dedup d
        LEFT JOIN art_dedup   a ON a.COD_ARTICOLO = d.COD_ARTICOLO
        LEFT JOIN categ_dedup c ON c.COD_CATEG    = a.COD_CATEG
        LEFT JOIN tipo_dedup  t ON t.COD_TIPO     = c.COD_TIPO
        ORDER BY cod_step, ordine
    """, [bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento)])
    return [ListaCaricaItem(**{k.lower(): v for k, v in r.items()}) for r in rows]


@router.post("", response_model=ListaCaricaItem, status_code=201)
async def add_articolo(id_evento: int, body: AddArticoloRequest) -> ListaCaricaItem:
    articolo = await fetch_articolo(body.cod_articolo)
    if not articolo:
        raise HTTPException(404, f"Articolo '{body.cod_articolo}' non trovato")

    ospiti = await get_ospiti(id_evento)
    qtadict = calcola_qta(articolo, ospiti)

    new_id = await get_next_id_lista()
    ordine = await get_next_ordine(id_evento)

    await insert("EVENTI_DET_PREL", {
        "ID_EVENTO":       id_evento,
        "ID":              new_id,
        "COD_ARTICOLO":    body.cod_articolo,
        "QTA":             1,
        "QTA_APE":         qtadict["qta_ape"],
        "QTA_SEDU":        qtadict["qta_sedu"],
        "QTA_BUFDOL":      qtadict["qta_bufdol"],
        "QTA_MAN_APE":     body.qta_man_ape,
        "QTA_MAN_SEDU":    body.qta_man_sedu,
        "QTA_MAN_BUFDOL":  body.qta_man_bufdol,
        "NOTE":            body.note,
        "ORDINE":          ordine,
    })

    return ListaCaricaItem(
        id=new_id,
        cod_articolo=body.cod_articolo,
        descrizione=articolo.get("DESCRIZIONE"),
        qta=1,
        qta_ape=qtadict["qta_ape"],
        qta_sedu=qtadict["qta_sedu"],
        qta_bufdol=qtadict["qta_bufdol"],
        qta_man_ape=body.qta_man_ape,
        qta_man_sedu=body.qta_man_sedu,
        qta_man_bufdol=body.qta_man_bufdol,
        note=body.note,
        ordine=ordine,
    )


@router.put("/{item_id}", response_model=dict)
async def update_articolo(
    id_evento: int, item_id: int, body: UpdateListaItemRequest
) -> dict:
    set_parts = [
        "QTA_MAN_APE    = @qma",
        "QTA_MAN_SEDU   = @qms",
        "QTA_MAN_BUFDOL = @qmb",
        "NOTE           = @note",
    ]
    params = [
        bigquery.ScalarQueryParameter("qma",       "FLOAT64", body.qta_man_ape),
        bigquery.ScalarQueryParameter("qms",       "FLOAT64", body.qta_man_sedu),
        bigquery.ScalarQueryParameter("qmb",       "FLOAT64", body.qta_man_bufdol),
        bigquery.ScalarQueryParameter("note",      "STRING",  body.note or ""),
        bigquery.ScalarQueryParameter("id_evento", "INT64",   id_evento),
        bigquery.ScalarQueryParameter("item_id",   "INT64",   item_id),
    ]
    # Sovrascrittura quantità base solo se esplicitamente passate
    if body.qta_ape is not None:
        set_parts.append("QTA_APE = @qa")
        params.append(bigquery.ScalarQueryParameter("qa", "FLOAT64", body.qta_ape))
    if body.qta_sedu is not None:
        set_parts.append("QTA_SEDU = @qs")
        params.append(bigquery.ScalarQueryParameter("qs", "FLOAT64", body.qta_sedu))
    if body.qta_bufdol is not None:
        set_parts.append("QTA_BUFDOL = @qb")
        params.append(bigquery.ScalarQueryParameter("qb", "FLOAT64", body.qta_bufdol))

    affected = await dml(f"""
        UPDATE {_table('EVENTI_DET_PREL')}
        SET {', '.join(set_parts)}
        WHERE CAST(ID_EVENTO AS INT64) = @id_evento
          AND CAST(ID AS INT64)        = @item_id
    """, params)
    if affected == 0:
        raise HTTPException(404, "Riga lista non trovata")
    return {"updated": item_id}


@router.delete("/{item_id}", response_model=dict)
async def remove_articolo(id_evento: int, item_id: int) -> dict:
    affected = await dml(f"""
        DELETE FROM {_table('EVENTI_DET_PREL')}
        WHERE CAST(ID_EVENTO AS INT64) = @id_evento
          AND CAST(ID AS INT64)        = @item_id
    """, [
        bigquery.ScalarQueryParameter("id_evento", "INT64", id_evento),
        bigquery.ScalarQueryParameter("item_id",   "INT64", item_id),
    ])
    if affected == 0:
        raise HTTPException(404, "Riga lista non trovata")
    return {"deleted": item_id}
