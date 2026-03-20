"""Router lookup — dati di riferimento per i dropdown del frontend."""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from db.bigquery import _table, insert, query
from models.articolo import ArticoloLookupItem
from models.evento import LocationItem, TipoEventoItem

router = APIRouter(prefix="/lookup", tags=["lookup"])


class LocationCreate(BaseModel):
    location: str


@router.get("/location", response_model=list[LocationItem])
async def get_location() -> list[LocationItem]:
    # Deduplica per ID, esclude righe senza nome location
    rows = await query(
        f"SELECT CAST(ID AS INT64) AS id, ANY_VALUE(LOCATION) AS location "
        f"FROM {_table('LOCATION')} "
        f"WHERE ID IS NOT NULL AND LOCATION IS NOT NULL "
        f"GROUP BY ID "
        f"ORDER BY location"
    )
    return [LocationItem(**r) for r in rows]


@router.post("/location", response_model=LocationItem, status_code=201)
async def create_location(body: LocationCreate) -> LocationItem:
    id_rows = await query(
        f"SELECT COALESCE(MAX(CAST(ID AS INT64)), 0) + 1 AS next_id FROM {_table('LOCATION')}"
    )
    new_id = int(id_rows[0]["next_id"])
    await insert("LOCATION", {"ID": new_id, "LOCATION": body.location.strip()})
    return LocationItem(id=new_id, location=body.location.strip())


@router.get("/articoli", response_model=list[ArticoloLookupItem])
async def get_articoli_disponibili() -> list[ArticoloLookupItem]:
    """Articoli con giacenza disponibile (QTA_GIAC > 0) per la lista di carico."""
    rows = await query(f"""
        SELECT
            COD_ARTICOLO                  AS cod_articolo,
            ANY_VALUE(DESCRIZIONE)        AS descrizione,
            ANY_VALUE(CAST(QTA_GIAC AS FLOAT64)) AS qta_giac
        FROM {_table('ARTICOLI')}
        WHERE QTA_GIAC IS NOT NULL AND CAST(QTA_GIAC AS FLOAT64) > 0
        GROUP BY COD_ARTICOLO
        ORDER BY cod_articolo
    """)
    return [ArticoloLookupItem(**r) for r in rows]


@router.get("/tipi-evento", response_model=list[TipoEventoItem])
async def get_tipi_evento() -> list[TipoEventoItem]:
    rows = await query(
        f"SELECT COD_TIPO AS cod_tipo, DESCRIZIONE AS descrizione, TIPO_PASTO AS tipo_pasto "
        f"FROM {_table('TB_TIPI_EVENTO')} ORDER BY DESCRIZIONE"
    )
    return [TipoEventoItem(**r) for r in rows]
