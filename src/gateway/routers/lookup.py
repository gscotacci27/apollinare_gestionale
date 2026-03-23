"""Router lookup — dati di riferimento per i dropdown del frontend."""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from db.bigquery import _table, insert, query
from models.articolo import ArticoloLookupItem, SezioneItem
from models.evento import LocationItem, TipoEventoItem

router = APIRouter(prefix="/lookup", tags=["lookup"])

_SEZIONI_ESCLUSE = (
    "'ELIM','FEE','LOC','PRELO','CONTAPO','FORNITORI',"
    "'DEGUS','OS','IP','ALLEG','PRS','VARIE'"
)


class LocationCreate(BaseModel):
    location: str


@router.get("/location", response_model=list[LocationItem])
async def get_location() -> list[LocationItem]:
    rows = await query(
        f"SELECT id, nome AS location "
        f"FROM {_table('location')} "
        f"WHERE id IS NOT NULL AND nome IS NOT NULL "
        f"ORDER BY nome"
    )
    return [LocationItem(**r) for r in rows]


@router.post("/location", response_model=LocationItem, status_code=201)
async def create_location(body: LocationCreate) -> LocationItem:
    id_rows = await query(
        f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {_table('location')}"
    )
    new_id = int(id_rows[0]["next_id"])
    nome = body.location.strip()
    await insert("location", {"id": new_id, "nome": nome, "is_active": True})
    return LocationItem(id=new_id, location=nome)


@router.get("/sezioni", response_model=list[SezioneItem])
async def get_sezioni() -> list[SezioneItem]:
    rows = await query(f"""
        WITH art_disp AS (
            SELECT cod_categ
            FROM {_table('articoli')}
            WHERE qta_giac IS NOT NULL AND qta_giac > 0
            GROUP BY cod_categ
        )
        SELECT t.cod_tipo, t.descrizione, t.cod_step
        FROM {_table('tb_tipi_mat')} t
        WHERE t.cod_tipo NOT IN ({_SEZIONI_ESCLUSE})
          AND EXISTS (
              SELECT 1 FROM {_table('tb_codici_categ')} c
              JOIN art_disp a ON a.cod_categ = c.cod_categ
              WHERE c.cod_tipo = t.cod_tipo
          )
        ORDER BY t.cod_step
    """)
    return [SezioneItem(**r) for r in rows]


@router.get("/articoli", response_model=list[ArticoloLookupItem])
async def get_articoli_disponibili() -> list[ArticoloLookupItem]:
    rows = await query(f"""
        SELECT
          a.cod_articolo,
          a.descrizione,
          a.qta_giac,
          c.cod_tipo,
          a.rank
        FROM {_table('articoli')} a
        LEFT JOIN {_table('tb_codici_categ')} c ON c.cod_categ = a.cod_categ
        WHERE a.qta_giac IS NOT NULL AND a.qta_giac > 0
        ORDER BY c.cod_tipo, a.rank NULLS LAST, a.cod_articolo
    """)
    return [ArticoloLookupItem(**r) for r in rows]


@router.get("/tipi-evento", response_model=list[TipoEventoItem])
async def get_tipi_evento() -> list[TipoEventoItem]:
    rows = await query(
        f"SELECT cod_tipo, descrizione, tipo_pasto "
        f"FROM {_table('tb_tipi_evento')} ORDER BY descrizione"
    )
    return [TipoEventoItem(**r) for r in rows]
