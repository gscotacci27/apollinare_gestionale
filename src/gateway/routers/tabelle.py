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
        SELECT l.id, l.nome AS location, COALESCE(u.n_eventi, 0) AS n_eventi
        FROM {_table('location')} l
        LEFT JOIN (
            SELECT id_location, COUNT(*) AS n_eventi
            FROM {_table('eventi')}
            WHERE id_location IS NOT NULL
              AND COALESCE(deleted, FALSE) = FALSE
            GROUP BY id_location
        ) u ON u.id_location = l.id
        WHERE l.id IS NOT NULL AND l.nome IS NOT NULL
        ORDER BY l.nome
    """)
    return rows


@router.get("/location/{id_location}/simili")
async def location_simili(id_location: int, soglia: float = 0.6) -> list[dict]:
    """Suggerisce location con nome simile (fuzzy match) per il merge."""
    rows = await query(f"""
        SELECT id, nome AS location
        FROM {_table('location')}
        WHERE id IS NOT NULL AND nome IS NOT NULL AND id != @id
        ORDER BY nome
    """, [bigquery.ScalarQueryParameter("id", "INT64", id_location)])

    source_rows = await query(
        f"SELECT nome AS location FROM {_table('location')} WHERE id = @id",
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
        f"UPDATE {_table('location')} SET nome = @loc WHERE id = @id",
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
        f"SELECT COUNT(*) AS cnt FROM {_table('eventi')} "
        f"WHERE id_location = @id AND COALESCE(deleted, FALSE) = FALSE",
        [bigquery.ScalarQueryParameter("id", "INT64", id_location)],
    )
    if int(uso[0]["cnt"] or 0) > 0:
        raise HTTPException(409, "Location usata da eventi esistenti, impossibile eliminare")
    await dml(
        f"DELETE FROM {_table('location')} WHERE id = @id",
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
        f"SELECT id FROM {_table('location')} WHERE id IN UNNEST(@ids)",
        [bigquery.ArrayQueryParameter("ids", "INT64", [source_id, target_id])],
    )
    found = {r["id"] for r in rows}
    for missing_id, label in [(source_id, "source"), (target_id, "target")]:
        if missing_id not in found:
            raise HTTPException(404, f"Location {label} {missing_id} non trovata")

    moved = await dml(
        f"UPDATE {_table('eventi')} SET id_location = @target WHERE id_location = @source",
        [
            bigquery.ScalarQueryParameter("target", "INT64", target_id),
            bigquery.ScalarQueryParameter("source", "INT64", source_id),
        ],
    )
    await dml(
        f"DELETE FROM {_table('location')} WHERE id = @source",
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
    perc_iva: float | None = None


@router.get("/articoli")
async def list_articoli_tabelle(search: str | None = None) -> list[dict]:
    """Lista articoli con sezione di appartenenza."""
    conditions: list[str] = []
    params: list = []
    if search:
        conditions.append("LOWER(a.descrizione) LIKE @search")
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await query(f"""
        SELECT
            a.cod_articolo,
            a.descrizione,
            a.cod_categ,
            c.cod_tipo,
            t.descrizione   AS tipo_desc,
            a.qta_giac,
            a.rank,
            a.coeff_a,
            a.coeff_s,
            a.coeff_b,
            a.qta_std_a,
            a.qta_std_s,
            a.qta_std_b,
            a.perc_ospiti,
            a.perc_iva,
            a.flg_qta_type
        FROM {_table('articoli')} a
        LEFT JOIN {_table('tb_codici_categ')} c ON c.cod_categ = a.cod_categ
        LEFT JOIN {_table('tb_tipi_mat')} t ON t.cod_tipo = c.cod_tipo
        {where}
        ORDER BY c.cod_tipo NULLS LAST, a.rank NULLS LAST, a.cod_articolo
    """, params)
    return rows


@router.patch("/articoli/{cod_articolo}", response_model=dict)
async def patch_articolo(cod_articolo: str, body: ArticoloPatch) -> dict:
    """Aggiorna i campi di un articolo."""
    set_clauses: list[str] = []
    params: list = [bigquery.ScalarQueryParameter("cod", "STRING", cod_articolo)]

    string_fields = [("descrizione", "descrizione"), ("cod_categ", "cod_categ")]
    float_fields = [
        ("qta_giac", "qta_giac"), ("rank", "rank"),
        ("coeff_a", "coeff_a"), ("coeff_s", "coeff_s"), ("coeff_b", "coeff_b"),
        ("qta_std_a", "qta_std_a"), ("qta_std_s", "qta_std_s"), ("qta_std_b", "qta_std_b"),
        ("perc_ospiti", "perc_ospiti"), ("perc_iva", "perc_iva"),
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
        f"UPDATE {_table('articoli')} SET {', '.join(set_clauses)} WHERE cod_articolo = @cod",
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
        f"SELECT COUNT(*) AS cnt FROM {_table('articoli')} WHERE cod_articolo = @cod",
        [bigquery.ScalarQueryParameter("cod", "STRING", cod)],
    )
    if int(existing[0]["cnt"] or 0) > 0:
        raise HTTPException(409, f"Articolo {cod!r} già esistente")
    await insert("articoli", {
        "cod_articolo": cod,
        "descrizione":  body.descrizione,
        "cod_categ":    body.cod_categ,
        "qta_giac":     body.qta_giac,
        "rank":         body.rank,
        "coeff_a":      body.coeff_a,
        "coeff_s":      body.coeff_s,
        "coeff_b":      body.coeff_b,
        "qta_std_a":    body.qta_std_a,
        "qta_std_s":    body.qta_std_s,
        "qta_std_b":    body.qta_std_b,
        "perc_ospiti":  body.perc_ospiti,
        "perc_iva":     body.perc_iva,
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
        SELECT
            t.cod_tipo,
            t.descrizione,
            t.cod_step,
            COALESCE(ac.n_articoli, 0) AS n_articoli
        FROM {_table('tb_tipi_mat')} t
        LEFT JOIN (
            SELECT c.cod_tipo, COUNT(DISTINCT a.cod_articolo) AS n_articoli
            FROM {_table('articoli')} a
            JOIN {_table('tb_codici_categ')} c ON c.cod_categ = a.cod_categ
            GROUP BY c.cod_tipo
        ) ac ON ac.cod_tipo = t.cod_tipo
        ORDER BY t.cod_step
    """)
    return rows


@router.patch("/sezioni/{cod_tipo}", response_model=dict)
async def patch_sezione(cod_tipo: str, body: SezionePatch) -> dict:
    """Aggiorna descrizione e/o ordine di una sezione."""
    set_clauses: list[str] = []
    params: list = [bigquery.ScalarQueryParameter("cod", "STRING", cod_tipo)]

    if body.descrizione is not None:
        set_clauses.append("descrizione = @desc")
        params.append(bigquery.ScalarQueryParameter("desc", "STRING", body.descrizione))
    if body.cod_step is not None:
        set_clauses.append("cod_step = @step")
        params.append(bigquery.ScalarQueryParameter("step", "INT64", body.cod_step))

    if not set_clauses:
        return {"updated": 0}

    affected = await dml(
        f"UPDATE {_table('tb_tipi_mat')} SET {', '.join(set_clauses)} WHERE cod_tipo = @cod",
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
        f"SELECT cod_tipo, descrizione FROM {_table('tb_tipi_ospiti')} ORDER BY cod_tipo"
    )
    return rows


@router.patch("/tipi-ospiti/{cod_tipo}", response_model=dict)
async def patch_tipo_ospite(cod_tipo: str, body: TipoOspitePatch) -> dict:
    if body.descrizione is None:
        return {"updated": 0}
    affected = await dml(
        f"UPDATE {_table('tb_tipi_ospiti')} SET descrizione = @desc WHERE cod_tipo = @cod",
        [
            bigquery.ScalarQueryParameter("desc", "STRING", body.descrizione),
            bigquery.ScalarQueryParameter("cod", "STRING", cod_tipo),
        ],
    )
    if affected == 0:
        raise HTTPException(404, f"Tipo ospite {cod_tipo!r} non trovato")
    invalidate_static()
    return {"updated": cod_tipo}
