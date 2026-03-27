from __future__ import annotations

import datetime as dt
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import asyncpg


PHASE2_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS phase2_raw_events (
        fingerprint TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        program TEXT NOT NULL,
        signature TEXT NOT NULL,
        slot BIGINT NOT NULL DEFAULT 0,
        log_index INTEGER NOT NULL DEFAULT 0,
        event_type TEXT NOT NULL,
        is_mint BOOLEAN NOT NULL DEFAULT FALSE,
        mint_id TEXT,
        owner TEXT,
        observed_at TIMESTAMPTZ NOT NULL,
        raw_logs JSONB NOT NULL DEFAULT '[]'::jsonb,
        parsed_payload JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_raw_events_signature
    ON phase2_raw_events(signature);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_raw_events_mint_observed
    ON phase2_raw_events(mint_id, observed_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_raw_events_owner_observed
    ON phase2_raw_events(owner, observed_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_mints (
        mint_id TEXT PRIMARY KEY,
        owner TEXT,
        name TEXT,
        symbol TEXT,
        bonding_curve TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        mint_sig TEXT,
        created_at TIMESTAMPTZ,
        last_event_fingerprint TEXT,
        last_event_slot BIGINT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_mint_snapshots (
        snapshot_id BIGSERIAL PRIMARY KEY,
        mint_id TEXT NOT NULL REFERENCES phase2_mints(mint_id) ON DELETE CASCADE,
        lifecycle_state TEXT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL,
        owner TEXT,
        name TEXT,
        symbol TEXT,
        bonding_curve TEXT,
        market_cap DOUBLE PRECISION,
        price_usd DOUBLE PRECISION,
        liquidity DOUBLE PRECISION,
        open_price DOUBLE PRECISION,
        high_price DOUBLE PRECISION,
        low_price DOUBLE PRECISION,
        current_price DOUBLE PRECISION,
        age_seconds DOUBLE PRECISION,
        tx_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        volume JSONB NOT NULL DEFAULT '{}'::jsonb,
        holders JSONB NOT NULL DEFAULT '{}'::jsonb,
        price_history JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_event_fingerprint TEXT,
        last_event_slot BIGINT,
        snapshot_payload JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_mint_snapshots_mint_time
    ON phase2_mint_snapshots(mint_id, recorded_at DESC);
    """,
]


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ensure_utc(value: dt.datetime | None) -> dt.datetime:
    if value is None:
        return _utc_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dt.datetime):
        return _ensure_utc(value).isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _jsonb_param(value: Any) -> str:
    return json.dumps(_json_safe(value), sort_keys=True, allow_nan=False)


def _canonical_payload(value: Any) -> str:
    return json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _finite_float_or(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    if isinstance(value, Decimal):
        if not value.is_finite():
            return default
        return float(value)
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(result):
        return default
    return result


def compute_event_fingerprint(
    *,
    source: str,
    signature: str,
    log_index: int,
    event_type: str,
    mint_id: str | None,
    payload: Any,
) -> str:
    seed = "|".join(
        [
            source,
            signature or "",
            str(log_index),
            event_type,
            mint_id or "",
            _canonical_payload(payload),
        ]
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


class Phase2Store:
    def __init__(self, db_dsn: str):
        self.db_dsn = db_dsn
        self.pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def _with_connection(self, operation):
        if self.pool is not None:
            async with self.pool.acquire() as conn:
                return await operation(conn)
        conn = await asyncpg.connect(self.db_dsn)
        try:
            return await operation(conn)
        finally:
            await conn.close()

    async def ensure_schema(self) -> None:
        async def _operation(conn):
            for statement in PHASE2_SCHEMA_STATEMENTS:
                await conn.execute(statement)

        await self._with_connection(_operation)

    async def record_raw_event(
        self,
        *,
        source: str,
        program: str,
        signature: str,
        slot: int,
        log_index: int,
        event_type: str,
        is_mint: bool,
        payload: dict[str, Any],
        raw_logs: list[str] | None = None,
        observed_at: dt.datetime | None = None,
    ) -> str:
        mint_id = str(payload.get("mint", "") or "")
        owner = str(payload.get("user") or payload.get("owner") or "")
        fingerprint = compute_event_fingerprint(
            source=source,
            signature=signature,
            log_index=log_index,
            event_type=event_type,
            mint_id=mint_id or None,
            payload=payload,
        )
        observed_at = _ensure_utc(observed_at)

        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_raw_events (
                    fingerprint,
                    source,
                    program,
                    signature,
                    slot,
                    log_index,
                    event_type,
                    is_mint,
                    mint_id,
                    owner,
                    observed_at,
                    raw_logs,
                    parsed_payload
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''),
                    $11, $12::jsonb, $13::jsonb
                )
                ON CONFLICT (fingerprint) DO NOTHING
                """,
                fingerprint,
                source,
                program,
                signature,
                int(slot or 0),
                int(log_index),
                event_type,
                bool(is_mint),
                mint_id,
                owner,
                observed_at,
                _jsonb_param(raw_logs or []),
                _jsonb_param(payload),
            )

        await self._with_connection(_operation)
        return fingerprint

    async def upsert_mint_record(
        self,
        *,
        mint_id: str,
        owner: str | None = None,
        name: str | None = None,
        symbol: str | None = None,
        bonding_curve: str | None = None,
        status: str = "active",
        mint_sig: str | None = None,
        created_at: dt.datetime | None = None,
        last_event_fingerprint: str | None = None,
        last_event_slot: int | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_mints (
                    mint_id,
                    owner,
                    name,
                    symbol,
                    bonding_curve,
                    status,
                    mint_sig,
                    created_at,
                    last_event_fingerprint,
                    last_event_slot,
                    updated_at
                )
                VALUES (
                    $1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''),
                    $6, NULLIF($7, ''), $8, NULLIF($9, ''), $10, $11
                )
                ON CONFLICT (mint_id) DO UPDATE SET
                    owner = COALESCE(EXCLUDED.owner, phase2_mints.owner),
                    name = COALESCE(EXCLUDED.name, phase2_mints.name),
                    symbol = COALESCE(EXCLUDED.symbol, phase2_mints.symbol),
                    bonding_curve = COALESCE(EXCLUDED.bonding_curve, phase2_mints.bonding_curve),
                    status = EXCLUDED.status,
                    mint_sig = COALESCE(EXCLUDED.mint_sig, phase2_mints.mint_sig),
                    created_at = COALESCE(EXCLUDED.created_at, phase2_mints.created_at),
                    last_event_fingerprint = COALESCE(EXCLUDED.last_event_fingerprint, phase2_mints.last_event_fingerprint),
                    last_event_slot = COALESCE(EXCLUDED.last_event_slot, phase2_mints.last_event_slot),
                    updated_at = EXCLUDED.updated_at
                """,
                mint_id,
                owner or "",
                name or "",
                symbol or "",
                bonding_curve or "",
                status,
                mint_sig or "",
                _ensure_utc(created_at) if created_at is not None else None,
                last_event_fingerprint or "",
                int(last_event_slot) if last_event_slot is not None else None,
                _utc_now(),
            )

        await self._with_connection(_operation)

    async def record_mint_snapshot(
        self,
        *,
        mint_id: str,
        lifecycle_state: str,
        snapshot: dict[str, Any],
        recorded_at: dt.datetime | None = None,
        last_event_fingerprint: str | None = None,
        last_event_slot: int | None = None,
    ) -> None:
        await self.upsert_mint_record(
            mint_id=mint_id,
            owner=str(snapshot.get("owner") or ""),
            name=str(snapshot.get("name") or ""),
            symbol=str(snapshot.get("symbol") or ""),
            bonding_curve=str(snapshot.get("bonding_curve") or ""),
            status=lifecycle_state,
            mint_sig=str(snapshot.get("mint_sig") or ""),
            created_at=snapshot.get("created_at"),
            last_event_fingerprint=last_event_fingerprint,
            last_event_slot=last_event_slot,
        )

        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_mint_snapshots (
                    mint_id,
                    lifecycle_state,
                    recorded_at,
                    owner,
                    name,
                    symbol,
                    bonding_curve,
                    market_cap,
                    price_usd,
                    liquidity,
                    open_price,
                    high_price,
                    low_price,
                    current_price,
                    age_seconds,
                    tx_counts,
                    volume,
                    holders,
                    price_history,
                    last_event_fingerprint,
                    last_event_slot,
                    snapshot_payload
                )
                VALUES (
                    $1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''),
                    $8, $9, $10, $11, $12, $13, $14, $15,
                    $16::jsonb, $17::jsonb, $18::jsonb, $19::jsonb,
                    NULLIF($20, ''), $21, $22::jsonb
                )
                """,
                mint_id,
                lifecycle_state,
                _ensure_utc(recorded_at),
                str(snapshot.get("owner") or ""),
                str(snapshot.get("name") or ""),
                str(snapshot.get("symbol") or ""),
                str(snapshot.get("bonding_curve") or ""),
                _finite_float_or(snapshot.get("market_cap"), 0.0),
                _finite_float_or(snapshot.get("price_usd"), 0.0),
                _finite_float_or(snapshot.get("liquidity"), 0.0),
                _finite_float_or(snapshot.get("open_price"), 0.0),
                _finite_float_or(snapshot.get("high_price"), 0.0),
                _finite_float_or(snapshot.get("low_price")),
                _finite_float_or(snapshot.get("current_price"), 0.0),
                _finite_float_or(snapshot.get("age"), 0.0),
                _jsonb_param(snapshot.get("tx_counts") or {}),
                _jsonb_param(snapshot.get("volume") or {}),
                _jsonb_param(snapshot.get("holders") or {}),
                _jsonb_param(snapshot.get("price_history") or {}),
                last_event_fingerprint or "",
                int(last_event_slot) if last_event_slot is not None else None,
                _jsonb_param(snapshot),
            )

        await self._with_connection(_operation)
