# Gestionale Apollinare — Development Progress

## Legend
| Symbol | Meaning |
|--------|---------|
| ✅ | Implemented and working |
| 🔧 | Stub — structure and interface defined, logic not yet implemented |
| 🔲 | Planned — not started |
| 🧪 | Tests written |

---

## Infrastructure & Config

| Module | Status | Description |
|--------|--------|-------------|
| `pyproject.toml` | ✅ | uv project config. Dependencies, Ruff, mypy, pytest. `src/` is the Python root. |
| `.pre-commit-config.yaml` | ✅ | Pre-commit hooks: check-yaml, check-json, editorconfig, ruff (fix + format + lint), mypy. |
| `deploy/Dockerfile` | ✅ | Python 3.12-slim image. Installs deps via uv, sets `PYTHONPATH=src`, runs uvicorn on port 8080. |
| `cloudbuild.yaml` | ✅ | Cloud Build pipeline: build image → push to Artifact Registry → deploy to Cloud Run. |
| `deploy/cloud_run_service.yaml` | ✅ | Cloud Run service definition. 0–3 instances, 512Mi, `sa-backend` service account. |

## Config

| Module | Status | Description |
|--------|--------|-------------|
| `src/config/settings.py` | ✅ | Pydantic-settings: `gcp_project_id`, `gcp_region`, `bq_dataset`. `@lru_cache` singleton. |

## Database

| Module | Status | Description |
|--------|--------|-------------|
| `src/db/bigquery.py` | ✅ | Async BigQuery helpers: `query()`, `dml()`, `insert()`, `insert_many()`. `_table()` resolves fully-qualified table names from `apollinare_legacy` dataset. |

## Models

| Module | Status | Description |
|--------|--------|-------------|
| `src/models/evento.py` | ✅ | Pydantic models: `Evento`, `EventoCreate`, `EventoUpdate`, `OspitiItem`, `AccontoItem`. |
| `src/models/articolo.py` | ✅ | Pydantic models: `Articolo`, `Categoria`, `Location`, `TipoEvento`. |

## Services

| Module | Status | Description |
|--------|--------|-------------|
| `src/services/calcolo_lista.py` | ✅ | Replica logica Oracle `F_LIST_PRELIEVO_ADD_ARTICOLO`. Calcola quantità lista di carico per tipo ospite (FLG_QTA_TYPE 1–5). |
| `src/services/calcolo_preventivo.py` | ✅ | Calcola preventivo economico dell'evento: ospiti × costo − sconti + acconti. |

## Gateway

| Module | Status | Description |
|--------|--------|-------------|
| `src/gateway/main.py` | ✅ | FastAPI entry point. Monta tutti i router. Structured JSON logging. `/health` liveness probe. |
| `src/gateway/routers/eventi.py` | ✅ | CRUD completo eventi + ospiti (upsert) + acconti + preventivo. |
| `src/gateway/routers/catalogo.py` | ✅ | Read-only: articoli, categorie, location, tipi evento. |
| `src/gateway/routers/lista_carico.py` | ✅ | Lista di carico: GET/PUT con calcolo automatico quantità via `calcolo_lista`. |
| `src/gateway/routers/reportistica.py` | ✅ | Report: consuntivo economico, impegni magazzino, acconti in scadenza. |

---

## Tests

| Module | Status | Description |
|--------|--------|-------------|
| `tests/conftest.py` | ✅ | `clear_settings_cache` (autouse) + `mock_settings`. |
| `tests/routers/` | 🔲 | Da implementare. |
| `tests/services/` | 🔲 | Da implementare. |

---

## Up next

1. Scrivere test per `routers/eventi.py` e `services/calcolo_lista.py`
2. Collegare Cloud Build trigger su `main` branch
