"""Router lookup — dati di riferimento per i dropdown del frontend."""
from __future__ import annotations

from fastapi import APIRouter

from db.bigquery import _table, query
from models.evento import LocationItem, TipoEventoItem

router = APIRouter(prefix="/lookup", tags=["lookup"])


@router.get("/location", response_model=list[LocationItem])
async def get_location() -> list[LocationItem]:
    # Alias sulla tabella per evitare ambiguità col nome colonna LOCATION
    rows = await query(
        f"SELECT CAST(loc.ID AS INT64) AS id, loc.LOCATION AS location "
        f"FROM {_table('LOCATION')} loc "
        f"WHERE loc.ID IS NOT NULL "
        f"ORDER BY loc.LOCATION"
    )
    return [LocationItem(**r) for r in rows]


@router.get("/tipi-evento", response_model=list[TipoEventoItem])
async def get_tipi_evento() -> list[TipoEventoItem]:
    rows = await query(
        f"SELECT COD_TIPO AS cod_tipo, DESCRIZIONE AS descrizione, TIPO_PASTO AS tipo_pasto "
        f"FROM {_table('TB_TIPI_EVENTO')} ORDER BY DESCRIZIONE"
    )
    return [TipoEventoItem(**r) for r in rows]
