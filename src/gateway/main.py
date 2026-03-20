"""Gestionale Apollinare Catering — FastAPI entry point.

Run locally::

    PYTHONPATH=src uvicorn gateway.main:app --reload --port 8081

Cloud Run::

    CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8080"]
"""
from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pythonjsonlogger import jsonlogger

from config.settings import get_settings
from services.cache import preload as cache_preload
from gateway.routers.catalogo import router as catalogo_router
from gateway.routers.eventi import router as eventi_router
from gateway.routers.lista_carico import router as lista_router
from gateway.routers.lookup import router as lookup_router
from gateway.routers.reportistica import router as report_router
from gateway.routers.scheda import router as scheda_router

# ── Structured JSON logging ───────────────────────────────────────────────────
_handler = logging.StreamHandler()
_handler.setFormatter(
    jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")
)
logging.root.addHandler(_handler)
logging.root.setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Gestionale Apollinare",
    description="API per la gestione operativa di eventi, liste di carico e preventivi.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(eventi_router)
app.include_router(lookup_router)
app.include_router(lista_router)
app.include_router(catalogo_router)
app.include_router(report_router)
app.include_router(scheda_router)


@app.get("/health")
async def health() -> dict:
    """Liveness probe for Cloud Run."""
    return {"status": "ok"}


@app.on_event("startup")
async def _startup() -> None:
    settings = get_settings()
    logger.info(
        "Gestionale starting",
        extra={
            "gcp_project": settings.gcp_project_id,
            "bq_dataset": settings.bq_dataset,
        },
    )
    await cache_preload()
