"""Pydantic models for Evento."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class OspitiItem(BaseModel):
    cod_tipo_ospite: str
    numero: int = 0
    costo: float = 0
    sconto: float = 0
    note: str | None = None
    ordine: int | None = None


class ListaCaricaItem(BaseModel):
    id: int
    cod_articolo: str
    descrizione: str | None = None
    qta: float = 0
    qta_ape: float = 0
    qta_sedu: float = 0
    qta_bufdol: float = 0
    qta_man_ape: float = 0
    qta_man_sedu: float = 0
    qta_man_bufdol: float = 0
    costo_articolo: float | None = None
    note: str | None = None
    ordine: int = 0


class AccontoItem(BaseModel):
    id: int
    id_evento: int
    acconto: float
    data_scadenza: str | None = None
    a_conferma: int = 0
    ordine: int = 0
    note: str | None = None


class EventoBase(BaseModel):
    descrizione: str | None = None
    cod_tipo: str | None = None
    cliente: str | None = None
    cliente_tel: str | None = None
    cliente_email: str | None = None
    indirizzo: str | None = None
    data: date | None = None
    ora_cerimonia: str | None = None
    ora_evento: str | None = None
    id_location: int | None = None
    stato: int = 100
    note: str | None = None
    allergie: str | None = None
    # mise en place
    sedia: str | None = None
    tovaglia: str | None = None
    tovagliolo: str | None = None
    runner: str | None = None
    sottopiatti: str | None = None
    piattino_pane: str | None = None
    posate: str | None = None
    bicchieri: str | None = None
    # menu testo
    primi: str | None = None
    secondi: str | None = None
    vini: str | None = None
    torta: str | None = None
    confettata: str | None = None
    stile_colori: str | None = None


class EventoCreate(EventoBase):
    pass


class EventoUpdate(EventoBase):
    pass


class Evento(EventoBase):
    id: int
    tot_ospiti: int | None = None
    deleted: int = 0
    disabled: int = 0
    is_template: int = 0
    # enriched fields (from view)
    location: str | None = None
    tipo_pasto: str | None = None
    descrizione_tipo: str | None = None
    color: str | None = None
    status: str | None = None

    class Config:
        from_attributes = True
