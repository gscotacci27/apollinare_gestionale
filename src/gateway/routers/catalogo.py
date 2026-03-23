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
        SELECT * FROM {_table('articoli')}
        {where}
        ORDER BY rank, cod_articolo
    """, params)
    # New schema already has lowercase column names
    return [Articolo(**r) for r in rows]


@router.get("/articoli/{cod_articolo}", response_model=Articolo)
async def get_articolo(cod_articolo: str):
    from fastapi import HTTPException
    rows = await query(f"""
        SELECT * FROM {_table('articoli')}
        WHERE cod_articolo = @cod
    """, [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)])
    if not rows:
        raise HTTPException(404, f"Articolo {cod_articolo!r} non trovato")
    return Articolo(**rows[0])


# ── Categorie ─────────────────────────────────────────────────────────────────

@router.get("/categorie", response_model=list[Categoria])
async def list_categorie(cod_tipo: str | None = None):
    conditions = []
    params: list = []
    if cod_tipo:
        conditions.append("cod_tipo = @cod_tipo")
        params.append(bigquery.ScalarQueryParameter("cod_tipo", "STRING", cod_tipo))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await query(f"SELECT * FROM {_table('tb_codici_categ')} {where} ORDER BY cod_categ", params)
    return [Categoria(**r) for r in rows]


@router.get("/tipi-materiale", response_model=list[TipoMateriale])
async def list_tipi_materiale():
    rows = await query(f"SELECT * FROM {_table('tb_tipi_mat')} ORDER BY cod_step")
    return [TipoMateriale(**r) for r in rows]


# ── Location ──────────────────────────────────────────────────────────────────

@router.get("/location")
async def list_location(search: str | None = None):
    conditions = []
    params: list = []
    if search:
        conditions.append("LOWER(nome) LIKE @search")
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return await query(f"SELECT * FROM {_table('location')} {where} ORDER BY nome", params)


@router.get("/location/{id_location}")
async def get_location(id_location: int):
    from fastapi import HTTPException
    rows = await query(f"""
        SELECT * FROM {_table('location')} WHERE id = @id
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_location)])
    if not rows:
        raise HTTPException(404, "Location non trovata")
    return rows[0]


# ── Tipi evento / ospiti ──────────────────────────────────────────────────────

@router.get("/tipi-evento")
async def list_tipi_evento():
    return await query(f"SELECT * FROM {_table('tb_tipi_evento')} ORDER BY cod_tipo")


@router.get("/tipi-ospiti")
async def list_tipi_ospiti():
    return await query(f"SELECT * FROM {_table('tb_tipi_ospiti')} ORDER BY cod_tipo")
