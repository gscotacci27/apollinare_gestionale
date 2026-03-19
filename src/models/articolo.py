"""Pydantic models for Articolo and related."""
from __future__ import annotations

from pydantic import BaseModel


class Articolo(BaseModel):
    cod_articolo: str
    descrizione: str | None = None
    cod_categ: str | None = None
    coeff: float | None = None
    coeff_a: float | None = None
    coeff_s: float | None = None
    coeff_b: float | None = None
    qta_std_a: float | None = None
    qta_std_s: float | None = None
    qta_std_b: float | None = None
    qta_giac: float | None = None
    perc_ospiti: float = 100
    perc_iva: float = 10
    flg_qta_type: str | None = None
    rank: float | None = None


class Categoria(BaseModel):
    cod_categ: str
    cod_tipo: str
    descrizione: str | None = None
    tipo_riepilogo: str | None = None
    show_print: int = 0


class TipoMateriale(BaseModel):
    cod_tipo: str
    descrizione: str | None = None


class AddArticoloRequest(BaseModel):
    cod_articolo: str
    # Quantità manuali (override rispetto al calcolo automatico)
    qta_man_ape: float = 0
    qta_man_sedu: float = 0
    qta_man_bufdol: float = 0
    note: str | None = None
