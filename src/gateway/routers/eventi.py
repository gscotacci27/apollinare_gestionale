"""Router: CRUD eventi + ospiti + acconti."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from db.bigquery import _table, dml, insert, query
from models.evento import Evento, EventoCreate, EventoUpdate, OspitiItem, AccontoItem
from services.calcolo_preventivo import get_preventivo

router = APIRouter(prefix="/eventi", tags=["eventi"])


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[Evento])
async def list_eventi(
    stato: int | None = Query(None, description="Filtra per stato (100/200/300/350/400)"),
    data_da: date | None = None,
    data_a: date | None = None,
    template: bool = False,
):
    conditions = ["CAST(e.deleted AS INT64) = 0", "CAST(e.disabled AS INT64) = 0"]
    params: list = []

    if not template:
        conditions.append("COALESCE(CAST(e.is_template AS INT64), 0) = 0")

    if stato is not None:
        conditions.append("CAST(e.stato AS INT64) = @stato")
        params.append(bigquery.ScalarQueryParameter("stato", "INT64", stato))

    if data_da:
        conditions.append("SAFE_CAST(e.data AS DATE) >= @data_da")
        params.append(bigquery.ScalarQueryParameter("data_da", "DATE", data_da.isoformat()))

    if data_a:
        conditions.append("SAFE_CAST(e.data AS DATE) <= @data_a")
        params.append(bigquery.ScalarQueryParameter("data_a", "DATE", data_a.isoformat()))

    where = " AND ".join(conditions)
    rows = await query(f"""
        WITH dedup AS (
          SELECT *
          FROM {_table('EVENTI')}
          QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(id AS INT64) ORDER BY id) = 1
        )
        SELECT e.*, l.location,
          CASE tp.tipo_pasto WHEN 'C' THEN 'Cena' WHEN 'P' THEN 'Pranzo' ELSE 'Altro' END AS tipo_pasto,
          tp.descrizione AS descrizione_tipo,
          vc.color, vc.status
        FROM dedup e
        LEFT JOIN {_table('LOCATION')} l ON e.id_location = l.id
        LEFT JOIN {_table('TB_TIPI_EVENTO')} tp ON tp.cod_tipo = e.cod_tipo
        LEFT JOIN {_table('VW_EVENT_COLOR')} vc ON vc.id = e.id
        WHERE {where}
        ORDER BY SAFE_CAST(e.data AS DATE) DESC
    """, params)
    return [Evento(**{k.lower(): v for k, v in r.items()}) for r in rows]


# ── GET ───────────────────────────────────────────────────────────────────────

@router.get("/{id_evento}", response_model=Evento)
async def get_evento(id_evento: int):
    rows = await query(f"""
        SELECT e.*, l.location,
          CASE tp.tipo_pasto WHEN 'C' THEN 'Cena' WHEN 'P' THEN 'Pranzo' ELSE 'Altro' END AS tipo_pasto,
          tp.descrizione AS descrizione_tipo,
          vc.color, vc.status
        FROM {_table('EVENTI')} e
        LEFT JOIN {_table('LOCATION')} l ON e.id_location = l.id
        LEFT JOIN {_table('TB_TIPI_EVENTO')} tp ON tp.cod_tipo = e.cod_tipo
        LEFT JOIN {_table('VW_EVENT_COLOR')} vc ON vc.id = e.id
        WHERE CAST(e.id AS INT64) = @id AND CAST(e.deleted AS INT64) = 0
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
    if not rows:
        raise HTTPException(404, f"Evento {id_evento} non trovato")
    return Evento(**{k.lower(): v for k, v in rows[0].items()})


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post("", response_model=dict, status_code=201)
async def create_evento(body: EventoCreate):
    # Get next ID
    id_rows = await query(f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {_table('EVENTI')}")
    new_id = int(id_rows[0]["next_id"])

    row = {
        "ID": new_id,
        "DESCRIZIONE": body.descrizione,
        "COD_TIPO": body.cod_tipo,
        "CLIENTE": body.cliente,
        "CLIENTE_TEL": body.cliente_tel,
        "CLIENTE_EMAIL": body.cliente_email,
        "INDIRIZZO": body.indirizzo,
        "DATA": body.data.isoformat() if body.data else None,
        "ID_LOCATION": body.id_location,
        "STATO": body.stato,
        "NOTE": body.note,
        "ALLERGIE": body.allergie,
        "SEDIA": body.sedia,
        "TOVAGLIA": body.tovaglia,
        "TOVAGLIOLO": body.tovagliolo,
        "RUNNER": body.runner,
        "SOTTOPIATTI": body.sottopiatti,
        "PIATTINO_PANE": body.piattino_pane,
        "POSATE": body.posate,
        "BICCHIERI": body.bicchieri,
        "PRIMI": body.primi,
        "SECONDI": body.secondi,
        "VINI": body.vini,
        "TORTA": body.torta,
        "CONFETTATA": body.confettata,
        "STILE_COLORI": body.stile_colori,
        "DELETED": 0,
        "DISABLED": 0,
        "IS_TEMPLATE": 0,
        "VERS_NUMBER": 0,
        "MAIL_ENABLED": 0,
        "CONTRATTO_FIRMATO": 0,
    }
    await insert("EVENTI", row)
    return {"id": new_id}


# ── UPDATE ────────────────────────────────────────────────────────────────────

@router.put("/{id_evento}", response_model=dict)
async def update_evento(id_evento: int, body: EventoUpdate):
    fields = body.model_dump(exclude_none=True)
    if not fields:
        raise HTTPException(400, "Nessun campo da aggiornare")

    set_parts = []
    params: list = [bigquery.ScalarQueryParameter("id", "INT64", id_evento)]
    for i, (k, v) in enumerate(fields.items()):
        param_name = f"p{i}"
        col = k.upper()
        if isinstance(v, date):
            set_parts.append(f"{col} = @{param_name}")
            params.append(bigquery.ScalarQueryParameter(param_name, "DATE", v.isoformat()))
        elif isinstance(v, int):
            set_parts.append(f"{col} = @{param_name}")
            params.append(bigquery.ScalarQueryParameter(param_name, "INT64", v))
        elif isinstance(v, float):
            set_parts.append(f"{col} = @{param_name}")
            params.append(bigquery.ScalarQueryParameter(param_name, "FLOAT64", v))
        else:
            set_parts.append(f"{col} = @{param_name}")
            params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(v)))

    sql = f"""
        UPDATE {_table('EVENTI')}
        SET {', '.join(set_parts)}
        WHERE id = @id
    """
    affected = await dml(sql, params)
    if affected == 0:
        raise HTTPException(404, f"Evento {id_evento} non trovato")
    return {"updated": affected}


# ── DELETE (soft) ─────────────────────────────────────────────────────────────

@router.delete("/{id_evento}", response_model=dict)
async def delete_evento(id_evento: int):
    affected = await dml(f"""
        UPDATE {_table('EVENTI')}
        SET deleted = 1
        WHERE id = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
    if affected == 0:
        raise HTTPException(404, f"Evento {id_evento} non trovato")
    return {"deleted": id_evento}


# ── OSPITI ────────────────────────────────────────────────────────────────────

@router.get("/{id_evento}/ospiti", response_model=list[OspitiItem])
async def get_ospiti(id_evento: int):
    rows = await query(f"""
        SELECT * FROM {_table('EVENTI_DET_OSPITI')}
        WHERE id_evento = @id
        ORDER BY ordine
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
    return [OspitiItem(**{k.lower(): v for k, v in r.items()}) for r in rows]


@router.put("/{id_evento}/ospiti", response_model=dict)
async def upsert_ospiti(id_evento: int, items: list[OspitiItem]):
    """Sostituisce tutti i tipi ospiti dell'evento."""
    # Delete existing
    await dml(f"""
        DELETE FROM {_table('EVENTI_DET_OSPITI')}
        WHERE id_evento = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])

    # Insert new
    rows = [
        {
            "ID_EVENTO": id_evento,
            "COD_TIPO_OSPITE": it.cod_tipo_ospite,
            "NUMERO": it.numero,
            "COSTO": it.costo,
            "SCONTO": it.sconto,
            "NOTE": it.note,
            "ORDINE": it.ordine,
        }
        for it in items
    ]
    if rows:
        from db.bigquery import insert_many
        await insert_many("EVENTI_DET_OSPITI", rows)

    # Update TOT_OSPITI on the event
    tot = sum(it.numero for it in items)
    await dml(f"""
        UPDATE {_table('EVENTI')} SET tot_ospiti = @tot WHERE id = @id
    """, [
        bigquery.ScalarQueryParameter("tot", "INT64", tot),
        bigquery.ScalarQueryParameter("id", "INT64", id_evento),
    ])
    return {"tot_ospiti": tot}


# ── ACCONTI ───────────────────────────────────────────────────────────────────

@router.get("/{id_evento}/acconti", response_model=list[AccontoItem])
async def get_acconti(id_evento: int):
    rows = await query(f"""
        SELECT * FROM {_table('EVENTI_ACCONTI')}
        WHERE id_evento = @id
        ORDER BY ordine
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_evento)])
    return [AccontoItem(**{k.lower(): v for k, v in r.items()}) for r in rows]


# ── PREVENTIVO ────────────────────────────────────────────────────────────────

@router.get("/{id_evento}/preventivo")
async def preventivo(id_evento: int):
    return await get_preventivo(id_evento)
