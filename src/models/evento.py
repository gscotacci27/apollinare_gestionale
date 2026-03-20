"""Pydantic models per SF-001 — Gestione Eventi."""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel, field_validator


class EventoCreate(BaseModel):
    descrizione: str
    data: date
    ora_evento: str | None = None
    id_location: int | None = None
    stato: int = 100          # 100=Preventivo, 200=In lavorazione, 400=Confermato
    cliente: str | None = None

    @field_validator("data")
    @classmethod
    def data_futura(cls, v: date) -> date:
        if v < date.today():
            raise ValueError("La data dell'evento deve essere futura")
        return v


class EventoResponse(BaseModel):
    id: int
    descrizione: str | None
    data: str | None
    ora_evento: str | None
    stato: int
    cliente: str | None
    id_location: int | None
    location_nome: str | None     # da JOIN con LOCATION
    tot_ospiti: int | None
    perc_sedute_aper: float | None = None   # % ospiti in piedi (aperitivo)

    model_config = {"from_attributes": True}


class PatchEventoRequest(BaseModel):
    stato: int | None = None
    descrizione: str | None = None
    cliente: str | None = None
    data: str | None = None        # ISO date "YYYY-MM-DD"
    ora_evento: str | None = None
    id_location: int | None = None
    tot_ospiti: int | None = None
    perc_sedute_aper: float | None = None


class LocationItem(BaseModel):
    id: int
    location: str


class TipoEventoItem(BaseModel):
    cod_tipo: str
    descrizione: str
    tipo_pasto: str | None


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
    note: str | None = None
    ordine: int = 0
    colore: str | None = None
    dimensioni: str | None = None
    # Sezione merceologica (da TB_TIPI_MAT via TB_CODICI_CATEG)
    cod_tipo: str | None = None
    tipo_descrizione: str | None = None
    cod_step: int = 999


class OspitiItem(BaseModel):
    cod_tipo_ospite: str
    numero: int = 0
    costo: float = 0
    sconto: float = 0
    note: str | None = None
    ordine: int | None = None


class AccontoItem(BaseModel):
    id: int
    id_evento: int
    acconto: float
    data_scadenza: str | None = None
    a_conferma: int = 0
    ordine: int = 0
    note: str | None = None
