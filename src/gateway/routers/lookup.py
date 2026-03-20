"""Router lookup — dati di riferimento per i dropdown del frontend."""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from db.bigquery import _table, insert, query
from models.articolo import ArticoloLookupItem, SezioneItem
from models.evento import LocationItem, TipoEventoItem

router = APIRouter(prefix="/lookup", tags=["lookup"])

# Sezioni amministrative/non operative da escludere dalla lista di carico
_SEZIONI_ESCLUSE = (
    "'ELIM','FEE','LOC','PRELO','CONTAPO','FORNITORI',"
    "'DEGUS','OS','IP','ALLEG','PRS','VARIE'"
)


class LocationCreate(BaseModel):
    location: str


@router.get("/location", response_model=list[LocationItem])
async def get_location() -> list[LocationItem]:
    rows = await query(
        f"SELECT CAST(ID AS INT64) AS id, ANY_VALUE(LOCATION) AS location "
        f"FROM {_table('LOCATION')} "
        f"WHERE ID IS NOT NULL AND LOCATION IS NOT NULL "
        f"GROUP BY ID ORDER BY location"
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


@router.get("/sezioni", response_model=list[SezioneItem])
async def get_sezioni() -> list[SezioneItem]:
    """Sezioni merceologiche operative (TB_TIPI_MAT) con articoli disponibili."""
    rows = await query(f"""
        WITH art_disp AS (
            SELECT ANY_VALUE(COD_CATEG) AS COD_CATEG
            FROM {_table('ARTICOLI')}
            WHERE QTA_GIAC IS NOT NULL AND CAST(QTA_GIAC AS FLOAT64) > 0
            GROUP BY COD_ARTICOLO
        ),
        categ_dedup AS (
            SELECT COD_CATEG, ANY_VALUE(COD_TIPO) AS COD_TIPO
            FROM {_table('TB_CODICI_CATEG')}
            GROUP BY COD_CATEG
        ),
        tipo_dedup AS (
            SELECT COD_TIPO,
                   ANY_VALUE(DESCRIZIONE)             AS DESCRIZIONE,
                   ANY_VALUE(CAST(COD_STEP AS INT64)) AS COD_STEP
            FROM {_table('TB_TIPI_MAT')}
            GROUP BY COD_TIPO
        )
        SELECT t.COD_TIPO AS cod_tipo, t.DESCRIZIONE AS descrizione, t.COD_STEP AS cod_step
        FROM tipo_dedup t
        WHERE t.COD_TIPO NOT IN ({_SEZIONI_ESCLUSE})
          AND EXISTS (
              SELECT 1 FROM categ_dedup c
              JOIN art_disp a ON a.COD_CATEG = c.COD_CATEG
              WHERE c.COD_TIPO = t.COD_TIPO
          )
        ORDER BY t.COD_STEP
    """)
    return [SezioneItem(**r) for r in rows]


@router.get("/articoli", response_model=list[ArticoloLookupItem])
async def get_articoli_disponibili() -> list[ArticoloLookupItem]:
    """Articoli disponibili (QTA_GIAC > 0) con sezione e rank per la lista di carico."""
    rows = await query(f"""
        WITH categ_dedup AS (
            SELECT COD_CATEG, ANY_VALUE(COD_TIPO) AS COD_TIPO
            FROM {_table('TB_CODICI_CATEG')}
            GROUP BY COD_CATEG
        )
        SELECT
            a.COD_ARTICOLO                         AS cod_articolo,
            ANY_VALUE(a.DESCRIZIONE)               AS descrizione,
            ANY_VALUE(CAST(a.QTA_GIAC AS FLOAT64)) AS qta_giac,
            ANY_VALUE(c.COD_TIPO)                  AS cod_tipo,
            ANY_VALUE(CAST(a.RANK AS FLOAT64))     AS rank
        FROM {_table('ARTICOLI')} a
        LEFT JOIN categ_dedup c ON c.COD_CATEG = a.COD_CATEG
        WHERE a.QTA_GIAC IS NOT NULL AND CAST(a.QTA_GIAC AS FLOAT64) > 0
        GROUP BY a.COD_ARTICOLO
        ORDER BY ANY_VALUE(c.COD_TIPO), ANY_VALUE(CAST(a.RANK AS FLOAT64)) NULLS LAST, a.COD_ARTICOLO
    """)
    return [ArticoloLookupItem(**r) for r in rows]


@router.get("/tipi-evento", response_model=list[TipoEventoItem])
async def get_tipi_evento() -> list[TipoEventoItem]:
    rows = await query(
        f"SELECT COD_TIPO AS cod_tipo, DESCRIZIONE AS descrizione, TIPO_PASTO AS tipo_pasto "
        f"FROM {_table('TB_TIPI_EVENTO')} ORDER BY DESCRIZIONE"
    )
    return [TipoEventoItem(**r) for r in rows]
