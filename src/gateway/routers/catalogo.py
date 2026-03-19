"""Router: articoli, categorie, location, tipi evento."""
from __future__ import annotations

from fastapi import APIRouter, Query
from google.cloud import bigquery

from db.bigquery import _table, query
from models.articolo import Articolo, Categoria, TipoMateriale

router = APIRouter(tags=["catalogo"])


# ── Articoli ──────────────────────────────────────────────────────────────────

@router.get("/articoli", response_model=list[Articolo])
async def list_articoli(
    cod_categ: str | None = None,
    search: str | None = Query(None, description="Ricerca per descrizione"),
):
    conditions = []
    params: list = []

    if cod_categ:
        conditions.append("cod_categ = @cod_categ")
        params.append(bigquery.ScalarQueryParameter("cod_categ", "STRING", cod_categ))

    if search:
        conditions.append("LOWER(descrizione) LIKE @search")
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await query(f"""
        SELECT * FROM {_table('ARTICOLI')}
        {where}
        ORDER BY rank, cod_articolo
    """, params)
    return [Articolo(**{k.lower(): v for k, v in r.items()}) for r in rows]


@router.get("/articoli/{cod_articolo}", response_model=Articolo)
async def get_articolo(cod_articolo: str):
    from fastapi import HTTPException
    rows = await query(f"""
        SELECT * FROM {_table('ARTICOLI')}
        WHERE cod_articolo = @cod
    """, [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)])
    if not rows:
        raise HTTPException(404, f"Articolo {cod_articolo!r} non trovato")
    return Articolo(**{k.lower(): v for k, v in rows[0].items()})


# ── Categorie ─────────────────────────────────────────────────────────────────

@router.get("/categorie", response_model=list[Categoria])
async def list_categorie(cod_tipo: str | None = None):
    conditions = []
    params: list = []
    if cod_tipo:
        conditions.append("cod_tipo = @cod_tipo")
        params.append(bigquery.ScalarQueryParameter("cod_tipo", "STRING", cod_tipo))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await query(f"SELECT * FROM {_table('TB_CODICI_CATEG')} {where} ORDER BY cod_categ", params)
    return [Categoria(**{k.lower(): v for k, v in r.items()}) for r in rows]


@router.get("/tipi-materiale", response_model=list[TipoMateriale])
async def list_tipi_materiale():
    rows = await query(f"SELECT * FROM {_table('TB_TIPI_MAT')} ORDER BY cod_step")
    return [TipoMateriale(**{k.lower(): v for k, v in r.items()}) for r in rows]


# ── Location ──────────────────────────────────────────────────────────────────

@router.get("/location")
async def list_location(search: str | None = None):
    conditions = []
    params: list = []
    if search:
        conditions.append("LOWER(location) LIKE @search")
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await query(f"SELECT * FROM {_table('LOCATION')} {where} ORDER BY location", params)
    return rows


@router.get("/location/{id_location}")
async def get_location(id_location: int):
    from fastapi import HTTPException
    rows = await query(f"""
        SELECT * FROM {_table('LOCATION')} WHERE id = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_location)])
    if not rows:
        raise HTTPException(404, "Location non trovata")
    return rows[0]


# ── Tipi evento / ospiti / pasto ──────────────────────────────────────────────

@router.get("/tipi-evento")
async def list_tipi_evento():
    rows = await query(f"SELECT * FROM {_table('TB_TIPI_EVENTO')} ORDER BY cod_tipo")
    return rows


@router.get("/tipi-ospiti")
async def list_tipi_ospiti():
    rows = await query(f"SELECT * FROM {_table('TB_TIPI_OSPITI')} ORDER BY cod_tipo")
    return rows
