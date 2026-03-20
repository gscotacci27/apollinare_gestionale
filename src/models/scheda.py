"""Pydantic models per SF-003 — Scheda Evento."""
from __future__ import annotations

from pydantic import BaseModel


class OspiteItem(BaseModel):
    cod_tipo: str
    descrizione: str | None
    numero: int = 0
    costo: float = 0
    sconto: float = 0
    note: str | None = None
    ordine: int = 0


class ExtraItem(BaseModel):
    id: int
    descrizione: str
    costo: float
    quantity: float = 1
    ordine: int = 0


class AccontoItem(BaseModel):
    id: int
    acconto: float
    data: str | None = None
    a_conferma: int = 0
    descrizione: str | None = None
    ordine: int = 0


class PreventivoCalc(BaseModel):
    ospiti_subtotale: float
    articoli_subtotale: float
    extra_subtotale: float
    totale_netto: float
    acconti_totale: float
    saldo: float


class SchedaResponse(BaseModel):
    ospiti: list[OspiteItem]
    extra: list[ExtraItem]
    acconti: list[AccontoItem]
    preventivo: PreventivoCalc


class PatchOspiteRequest(BaseModel):
    numero: int
    costo: float
    sconto: float = 0
    note: str | None = None


class AddExtraRequest(BaseModel):
    descrizione: str
    costo: float
    quantity: float = 1


class AddAccontoRequest(BaseModel):
    acconto: float
    data: str | None = None
    a_conferma: int = 0
    descrizione: str | None = None
