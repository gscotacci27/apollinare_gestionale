"""BigQuery client and query helpers."""
from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime
from typing import Any

from google.cloud import bigquery

from config.settings import get_settings

_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        s = get_settings()
        _client = bigquery.Client(project=s.gcp_project_id)
    return _client


def _table(name: str) -> str:
    s = get_settings()
    return f"`{s.gcp_project_id}.{s.bq_dataset}.{name}`"


def _serialize(val: Any) -> Any:
    """Convert Python types to BQ-JSON-compatible values."""
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    return val


def _row_to_dict(row: bigquery.Row) -> dict:
    d = dict(row.items())
    return {k: _serialize(v) for k, v in d.items()}


# ── Query helpers ─────────────────────────────────────────────────────────────

async def query(sql: str, params: list | None = None) -> list[dict]:
    """Run a SELECT and return list of dicts."""
    def _run() -> list[dict]:
        client = get_client()
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        result = client.query(sql, job_config=job_config).result()
        return [_row_to_dict(r) for r in result]
    return await asyncio.to_thread(_run)


async def insert(table: str, row: dict) -> None:
    """Streaming insert a single row."""
    def _run() -> None:
        client = get_client()
        errors = client.insert_rows_json(_table(table).strip("`"), [row])
        if errors:
            raise RuntimeError(f"BQ insert error: {errors}")
    await asyncio.to_thread(_run)


async def insert_many(table: str, rows: list[dict]) -> None:
    """Streaming insert multiple rows in batches of 500."""
    def _run() -> None:
        client = get_client()
        tbl_ref = _table(table).strip("`")
        for i in range(0, len(rows), 500):
            errors = client.insert_rows_json(tbl_ref, rows[i:i + 500])
            if errors:
                raise RuntimeError(f"BQ insert error: {errors}")
    await asyncio.to_thread(_run)


async def dml(sql: str, params: list | None = None) -> int:
    """Run an UPDATE/DELETE/INSERT DML and return rows affected."""
    def _run() -> int:
        client = get_client()
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        job = client.query(sql, job_config=job_config)
        job.result()
        return job.num_dml_affected_rows or 0
    return await asyncio.to_thread(_run)


def new_id() -> str:
    return str(uuid.uuid4())
