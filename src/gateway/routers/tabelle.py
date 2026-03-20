"""Router tabelle statiche — Location, Articoli, Sezioni, Tipi Ospiti.

CRUD + merge/fuzzy per le tabelle di configurazione che alimentano i calcoli.
"""
from __future__ import annotations

import difflib

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery
from pydantic import BaseModel

from db.bigquery import _table, dml, insert, query
from services.cache import invalidate_lista_all, invalidate_static

router = APIRouter(prefix="/tabelle", tags=["tabelle"])


# ══════════════════════════════════════════════════════════════════════════════
# LOCATION
# ══════════════════════════════════════════════════════════════════════════════

class LocationPatch(BaseModel):
    location: str


class LocationMergeRequest(BaseModel):
    target_id: int   # ID da mantenere; source verrà eliminato


@router.get("/location")
async def list_location_con_uso() -> list[dict]:
    """Location con conteggio eventi associati e lista simili (fuzzy)."""
    rows = await query(f"""
        WITH loc AS (
            SELECT CAST(ID AS INT64) AS id, ANY_VALUE(LOCATION) AS location
            FROM {_table('LOCATION')}
            WHERE ID IS NOT NULL AND LOCATION IS NOT NULL
            GROUP BY ID
        ),
        uso AS (
            SELECT CAST(ID_LOCATION AS INT64) AS id_location, COUNT(*) AS n_eventi
            FROM {_table('EVENTI')}
            WHERE ID_LOCATION IS NOT NULL
              AND COALESCE(CAST(DELETED AS INT64), 0) = 0
            GROUP BY ID_LOCATION
        )
        SELECT l.id, l.location, COALESCE(u.n_eventi, 0) AS n_eventi
        FROM loc l
        LEFT JOIN uso u ON u.id_location = l.id
        ORDER BY l.location
    """)
    return rows


@router.get("/location/{id_location}/simili")
async def location_simili(id_location: int, soglia: float = 0.6) -> list[dict]:
    """Suggerisce location con nome simile (fuzzy match) per il merge."""
    rows = await query(f"""
        SELECT CAST(ID AS INT64) AS id, ANY_VALUE(LOCATION) AS location
        FROM {_table('LOCATION')}
        WHERE ID IS NOT NULL AND LOCATION IS NOT NULL AND CAST(ID AS INT64) != @id
        GROUP BY ID
        ORDER BY location
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_location)])

    # Prendi il nome della location di riferimento
    source_rows = await query(
        f"SELECT ANY_VALUE(LOCATION) AS location FROM {_table('LOCATION')} "
        f"WHERE CAST(ID AS INT64) = @id AND LOCATION IS NOT NULL GROUP BY ID",
        [bigquery.ScalarQueryParameter("id", "INT64", id_location)],
    )
    if not source_rows:
        raise HTTPException(404, f"Location {id_location} non trovata")
    source_name: str = source_rows[0]["location"] or ""

    simili = []
    for r in rows:
        nome = r["location"] or ""
        ratio = difflib.SequenceMatcher(
            None,
            source_name.lower().strip(),
            nome.lower().strip(),
        ).ratio()
        if ratio >= soglia:
            simili.append({"id": r["id"], "location": nome, "similarita": round(ratio, 2)})

    simili.sort(key=lambda x: x["similarita"], reverse=True)
    return simili[:10]


@router.patch("/location/{id_location}", response_model=dict)
async def rename_location(id_location: int, body: LocationPatch) -> dict:
    """Rinomina una location."""
    name = body.location.strip()
    if not name:
        raise HTTPException(400, "Il nome non può essere vuoto")
    affected = await dml(
        f"UPDATE {_table('LOCATION')} SET LOCATION = @loc WHERE CAST(ID AS INT64) = @id",
        [
            bigquery.ScalarQueryParameter("loc", "STRING", name),
            bigquery.ScalarQueryParameter("id", "INT64", id_location),
        ],
    )
    if affected == 0:
        raise HTTPException(404, f"Location {id_location} non trovata")
    return {"updated": id_location, "location": name}


@router.delete("/location/{id_location}", response_model=dict)
async def delete_location(id_location: int) -> dict:
    """Elimina una location solo se non usata da eventi."""
    uso = await query(
        f"SELECT COUNT(*) AS cnt FROM {_table('EVENTI')} "
        f"WHERE CAST(ID_LOCATION AS INT64) = @id AND COALESCE(CAST(DELETED AS INT64),0) = 0",
        [bigquery.ScalarQueryParameter("id", "INT64", id_location)],
    )
    if int(uso[0]["cnt"] or 0) > 0:
        raise HTTPException(409, "Location usata da eventi esistenti, impossibile eliminare")
    await dml(
        f"DELETE FROM {_table('LOCATION')} WHERE CAST(ID AS INT64) = @id",
        [bigquery.ScalarQueryParameter("id", "INT64", id_location)],
    )
    return {"deleted": id_location}


@router.post("/location/{id_location}/merge", response_model=dict)
async def merge_location(id_location: int, body: LocationMergeRequest) -> dict:
    """Unisce due location: riassegna gli eventi da source → target, elimina source."""
    source_id = id_location
    target_id = body.target_id
    if source_id == target_id:
        raise HTTPException(400, "Source e target devono essere diversi")

    rows = await query(
        f"SELECT CAST(ID AS INT64) AS id FROM {_table('LOCATION')} "
        f"WHERE CAST(ID AS INT64) IN UNNEST(@ids) GROUP BY ID",
        [bigquery.ArrayQueryParameter("ids", "INT64", [source_id, target_id])],
    )
    found = {r["id"] for r in rows}
    for missing_id, label in [(source_id, "source"), (target_id, "target")]:
        if missing_id not in found:
            raise HTTPException(404, f"Location {label} {missing_id} non trovata")

    moved = await dml(
        f"UPDATE {_table('EVENTI')} SET ID_LOCATION = @target WHERE CAST(ID_LOCATION AS INT64) = @source",
        [
            bigquery.ScalarQueryParameter("target", "INT64", target_id),
            bigquery.ScalarQueryParameter("source", "INT64", source_id),
        ],
    )
    await dml(
        f"DELETE FROM {_table('LOCATION')} WHERE CAST(ID AS INT64) = @source",
        [bigquery.ScalarQueryParameter("source", "INT64", source_id)],
    )
    return {"merged": source_id, "into": target_id, "eventi_spostati": moved}


# ══════════════════════════════════════════════════════════════════════════════
# ARTICOLI
# ══════════════════════════════════════════════════════════════════════════════

class ArticoloPatch(BaseModel):
    descrizione: str | None = None
    qta_giac: float | None = None
    rank: float | None = None
    cod_categ: str | None = None
    coeff: float | None = None
    coeff_a: float | None = None
    coeff_s: float | None = None
    coeff_b: float | None = None
    qta_std_a: float | None = None
    qta_std_s: float | None = None
    qta_std_b: float | None = None
    perc_ospiti: float | None = None
    perc_iva: float | None = None


class ArticoloCreate(BaseModel):
    cod_articolo: str
    descrizione: str
    cod_categ: str | None = None
    qta_giac: float = 0
    rank: float | None = None
    coeff_a: float | None = None
    coeff_s: float | None = None
    coeff_b: float | None = None
    qta_std_a: float | None = None
    qta_std_s: float | None = None
    qta_std_b: float | None = None
    perc_ospiti: float = 100
    perc_iva: float = 10


@router.get("/articoli")
async def list_articoli_tabelle(search: str | None = None) -> list[dict]:
    """Lista articoli con sezione di appartenenza."""
    conditions: list[str] = []
    params: list = []
    if search:
        conditions.append("LOWER(a.DESCRIZIONE) LIKE @search")
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await query(f"""
        WITH categ_dedup AS (
            SELECT COD_CATEG, ANY_VALUE(COD_TIPO) AS COD_TIPO
            FROM {_table('TB_CODICI_CATEG')}
            GROUP BY COD_CATEG
        ),
        tipo_dedup AS (
            SELECT COD_TIPO, ANY_VALUE(DESCRIZIONE) AS TIPO_DESC
            FROM {_table('TB_TIPI_MAT')}
            GROUP BY COD_TIPO
        )
        SELECT
            a.COD_ARTICOLO                              AS cod_articolo,
            ANY_VALUE(a.DESCRIZIONE)                    AS descrizione,
            ANY_VALUE(a.COD_CATEG)                      AS cod_categ,
            ANY_VALUE(c.COD_TIPO)                       AS cod_tipo,
            ANY_VALUE(t.TIPO_DESC)                      AS tipo_desc,
            ANY_VALUE(CAST(a.QTA_GIAC  AS FLOAT64))     AS qta_giac,
            ANY_VALUE(CAST(a.RANK      AS FLOAT64))     AS rank,
            ANY_VALUE(CAST(a.COEFF_A   AS FLOAT64))     AS coeff_a,
            ANY_VALUE(CAST(a.COEFF_S   AS FLOAT64))     AS coeff_s,
            ANY_VALUE(CAST(a.COEFF_B   AS FLOAT64))     AS coeff_b,
            ANY_VALUE(CAST(a.QTA_STD_A AS FLOAT64))     AS qta_std_a,
            ANY_VALUE(CAST(a.QTA_STD_S AS FLOAT64))     AS qta_std_s,
            ANY_VALUE(CAST(a.QTA_STD_B AS FLOAT64))     AS qta_std_b,
            ANY_VALUE(CAST(a.PERC_OSPITI AS FLOAT64))   AS perc_ospiti,
            ANY_VALUE(CAST(a.PERC_IVA  AS FLOAT64))     AS perc_iva,
            ANY_VALUE(a.FLG_QTA_TYPE)                   AS flg_qta_type
        FROM {_table('ARTICOLI')} a
        LEFT JOIN categ_dedup c ON c.COD_CATEG = a.COD_CATEG
        LEFT JOIN tipo_dedup  t ON t.COD_TIPO  = c.COD_TIPO
        {where}
        GROUP BY a.COD_ARTICOLO
        ORDER BY ANY_VALUE(c.COD_TIPO) NULLS LAST,
                 ANY_VALUE(CAST(a.RANK AS FLOAT64)) NULLS LAST,
                 a.COD_ARTICOLO
    """, params)
    return rows


@router.patch("/articoli/{cod_articolo}", response_model=dict)
async def patch_articolo(cod_articolo: str, body: ArticoloPatch) -> dict:
    """Aggiorna i campi di un articolo."""
    set_clauses: list[str] = []
    params: list = [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)]

    string_fields = [("descrizione", "DESCRIZIONE"), ("cod_categ", "COD_CATEG")]
    float_fields = [
        ("qta_giac", "QTA_GIAC"), ("rank", "RANK"), ("coeff", "COEFF"),
        ("coeff_a", "COEFF_A"), ("coeff_s", "COEFF_S"), ("coeff_b", "COEFF_B"),
        ("qta_std_a", "QTA_STD_A"), ("qta_std_s", "QTA_STD_S"), ("qta_std_b", "QTA_STD_B"),
        ("perc_ospiti", "PERC_OSPITI"), ("perc_iva", "PERC_IVA"),
    ]

    for fname, col in string_fields:
        val = getattr(body, fname)
        if val is not None:
            set_clauses.append(f"{col} = @{fname}")
            params.append(bigquery.ScalarQueryParameter(fname, "STRING", val))

    for fname, col in float_fields:
        val = getattr(body, fname)
        if val is not None:
            set_clauses.append(f"{col} = @{fname}")
            params.append(bigquery.ScalarQueryParameter(fname, "FLOAT64", val))

    if not set_clauses:
        return {"updated": 0}

    affected = await dml(
        f"UPDATE {_table('ARTICOLI')} SET {', '.join(set_clauses)} WHERE COD_ARTICOLO = @cod",
        params,
    )
    if affected == 0:
        raise HTTPException(404, f"Articolo {cod_articolo!r} non trovato")
    invalidate_static()
    invalidate_lista_all()
    return {"updated": cod_articolo}


@router.post("/articoli", response_model=dict, status_code=201)
async def create_articolo(body: ArticoloCreate) -> dict:
    """Crea un nuovo articolo."""
    cod = body.cod_articolo.strip().upper()
    existing = await query(
        f"SELECT COUNT(*) AS cnt FROM {_table('ARTICOLI')} WHERE COD_ARTICOLO = @cod",
        [bigquery.ScalarQueryParameter("cod", "STRING", cod)],
    )
    if int(existing[0]["cnt"] or 0) > 0:
        raise HTTPException(409, f"Articolo {cod!r} già esistente")
    await insert("ARTICOLI", {
        "COD_ARTICOLO": cod,
        "DESCRIZIONE":  body.descrizione,
        "COD_CATEG":    body.cod_categ,
        "QTA_GIAC":     body.qta_giac,
        "RANK":         body.rank,
        "COEFF_A":      body.coeff_a,
        "COEFF_S":      body.coeff_s,
        "COEFF_B":      body.coeff_b,
        "QTA_STD_A":    body.qta_std_a,
        "QTA_STD_S":    body.qta_std_s,
        "QTA_STD_B":    body.qta_std_b,
        "PERC_OSPITI":  body.perc_ospiti,
        "PERC_IVA":     body.perc_iva,
    })
    invalidate_static()
    return {"created": cod}


# ══════════════════════════════════════════════════════════════════════════════
# SEZIONI (TB_TIPI_MAT)
# ══════════════════════════════════════════════════════════════════════════════

class SezionePatch(BaseModel):
    descrizione: str | None = None
    cod_step: int | None = None


@router.get("/sezioni")
async def list_sezioni_tabelle() -> list[dict]:
    """Tutte le sezioni merceologiche con conteggio articoli associati."""
    rows = await query(f"""
        WITH tipo_dedup AS (
            SELECT COD_TIPO,
                   ANY_VALUE(DESCRIZIONE)             AS descrizione,
                   ANY_VALUE(CAST(COD_STEP AS INT64)) AS cod_step
            FROM {_table('TB_TIPI_MAT')}
            GROUP BY COD_TIPO
        ),
        categ_dedup AS (
            SELECT COD_CATEG, ANY_VALUE(COD_TIPO) AS COD_TIPO
            FROM {_table('TB_CODICI_CATEG')}
            GROUP BY COD_CATEG
        ),
        art_count AS (
            SELECT c.COD_TIPO, COUNT(DISTINCT a.COD_ARTICOLO) AS n_articoli
            FROM {_table('ARTICOLI')} a
            JOIN categ_dedup c ON c.COD_CATEG = a.COD_CATEG
            GROUP BY c.COD_TIPO
        )
        SELECT t.COD_TIPO AS cod_tipo, t.descrizione, t.cod_step,
               COALESCE(ac.n_articoli, 0) AS n_articoli
        FROM tipo_dedup t
        LEFT JOIN art_count ac ON ac.COD_TIPO = t.COD_TIPO
        ORDER BY t.cod_step
    """)
    return rows


@router.patch("/sezioni/{cod_tipo}", response_model=dict)
async def patch_sezione(cod_tipo: str, body: SezionePatch) -> dict:
    """Aggiorna descrizione e/o ordine di una sezione."""
    set_clauses: list[str] = []
    params: list = [bigquery.ScalarQueryParameter("cod", "STRING", cod_tipo)]

    if body.descrizione is not None:
        set_clauses.append("DESCRIZIONE = @desc")
        params.append(bigquery.ScalarQueryParameter("desc", "STRING", body.descrizione))
    if body.cod_step is not None:
        set_clauses.append("COD_STEP = @step")
        params.append(bigquery.ScalarQueryParameter("step", "INT64", body.cod_step))

    if not set_clauses:
        return {"updated": 0}

    affected = await dml(
        f"UPDATE {_table('TB_TIPI_MAT')} SET {', '.join(set_clauses)} WHERE COD_TIPO = @cod",
        params,
    )
    if affected == 0:
        raise HTTPException(404, f"Sezione {cod_tipo!r} non trovata")
    invalidate_static()
    return {"updated": cod_tipo}


# ══════════════════════════════════════════════════════════════════════════════
# TIPI OSPITI (TB_TIPI_OSPITI)
# ══════════════════════════════════════════════════════════════════════════════

class TipoOspitePatch(BaseModel):
    descrizione: str | None = None


@router.get("/tipi-ospiti")
async def list_tipi_ospiti_tabelle() -> list[dict]:
    rows = await query(
        f"SELECT COD_TIPO AS cod_tipo, ANY_VALUE(DESCRIZIONE) AS descrizione "
        f"FROM {_table('TB_TIPI_OSPITI')} GROUP BY COD_TIPO ORDER BY COD_TIPO"
    )
    return rows


@router.patch("/tipi-ospiti/{cod_tipo}", response_model=dict)
async def patch_tipo_ospite(cod_tipo: str, body: TipoOspitePatch) -> dict:
    if body.descrizione is None:
        return {"updated": 0}
    affected = await dml(
        f"UPDATE {_table('TB_TIPI_OSPITI')} SET DESCRIZIONE = @desc WHERE COD_TIPO = @cod",
        [
            bigquery.ScalarQueryParameter("desc", "STRING", body.descrizione),
            bigquery.ScalarQueryParameter("cod", "STRING", cod_tipo),
        ],
    )
    if affected == 0:
        raise HTTPException(404, f"Tipo ospite {cod_tipo!r} non trovato")
    invalidate_static()
    return {"updated": cod_tipo}
