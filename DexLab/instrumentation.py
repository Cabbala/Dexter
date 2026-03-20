from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import socket
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path, PureWindowsPath
from threading import Lock
from typing import Any


LOGGER = logging.getLogger("dexter.vexter")

FIXED_WINDOWS_ROOT = PureWindowsPath(r"C:\Users\bot\quant\Vexter")
PREFERRED_WINDOWS_ROOTS = (
    PureWindowsPath(r"D:\Quant\Vexter"),
    FIXED_WINDOWS_ROOT,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime | None = None) -> str:
    value = value or utc_now()
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def as_float(value: Any) -> float:
    if value in (None, "", "Infinity"):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def pct_change(basis: Any, value: Any) -> float:
    basis_value = as_float(basis)
    current_value = as_float(value)
    if basis_value <= 0:
        return 0.0
    return ((current_value - basis_value) / basis_value) * 100.0


def mask_secret(value: str | None) -> str | None:
    if not value:
        return value
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime,)):
        return isoformat_utc(value)
    if isinstance(value, (Path, PureWindowsPath)):
        return str(value)
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=json_default)
        handle.write("\n")


class RuntimeLayout:
    def __init__(self, output_root: str | os.PathLike[str] | None = None, runtime_root: str | None = None):
        self.runtime_root = runtime_root or os.getenv("VEXTER_RUNTIME_ROOT", str(FIXED_WINDOWS_ROOT))
        self.output_root = self._resolve_output_root(output_root)
        self.config_dir = self.output_root / "runtime" / "dexter" / "config"
        self.export_dir = self.output_root / "runtime" / "dexter" / "export"
        self.raw_dir = self.output_root / "data" / "raw" / "dexter"
        self.logs_dir = self.output_root / "data" / "logs" / "dexter"
        self.replays_dir = self.output_root / "data" / "replays" / "dexter"
        self.db_dir = self.output_root / "data" / "postgres" / "dexter"

    @staticmethod
    def _resolve_output_root(output_root: str | os.PathLike[str] | None) -> Path:
        if output_root:
            return Path(output_root)

        env_root = os.getenv("VEXTER_OUTPUT_ROOT")
        if env_root:
            return Path(env_root)

        if platform.system() == "Windows":
            for candidate in PREFERRED_WINDOWS_ROOTS:
                candidate_path = Path(str(candidate))
                if candidate_path.exists():
                    return candidate_path
            return Path(str(FIXED_WINDOWS_ROOT))

        return Path("dev") / "vexter_runtime"

    def ensure(self) -> None:
        for path in (
            self.config_dir,
            self.export_dir,
            self.raw_dir,
            self.logs_dir,
            self.replays_dir,
            self.db_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def metadata(self) -> dict[str, str]:
        root = PureWindowsPath(self.runtime_root)
        return {
            "runtime_root": str(root),
            "config_dir": str(root / "runtime" / "dexter" / "config"),
            "export_dir": str(root / "runtime" / "dexter" / "export"),
            "raw_events_dir": str(root / "data" / "raw" / "dexter"),
            "logs_dir": str(root / "data" / "logs" / "dexter"),
            "replays_dir": str(root / "data" / "replays" / "dexter"),
            "db_exports_dir": str(root / "data" / "postgres" / "dexter"),
        }


class ReplayExporter:
    def __init__(self, output_root: str | os.PathLike[str] | None = None, runtime_root: str | None = None):
        self.layout = RuntimeLayout(output_root=output_root, runtime_root=runtime_root)
        self.layout.ensure()

    def export_record(self, mint_id: str, payload: dict[str, Any]) -> Path:
        export_path = self.layout.replays_dir / "stagnant" / f"{mint_id}.json"
        write_json(
            export_path,
            {
                "mint_id": mint_id,
                "exported_at_utc": isoformat_utc(),
                "runtime_layout": self.layout.metadata(),
                "payload": payload,
            },
        )
        return export_path


class DexterInstrumentation:
    def __init__(
        self,
        source_repo_root: str | os.PathLike[str],
        mode: str | None = None,
        transport_mode: str | None = None,
        runtime_root: str | None = None,
        output_root: str | os.PathLike[str] | None = None,
        source_commit: str | None = None,
    ):
        self.source_repo_root = Path(source_repo_root)
        self.mode = mode or os.getenv("VEXTER_MODE", "observe_live")
        self.transport_mode = transport_mode or os.getenv("VEXTER_TRANSPORT_MODE", "ws")
        self.layout = RuntimeLayout(output_root=output_root, runtime_root=runtime_root)
        self.layout.ensure()
        self.source_commit = source_commit or self._resolve_source_commit()
        self.run_id = os.getenv(
            "VEXTER_RUN_ID",
            f"dexter-{socket.gethostname().lower()}-{utc_now():%Y%m%dT%H%M%SZ}",
        )
        self.started_at = utc_now()
        self.ended_at: datetime | None = None
        self.host_role = "windows_runtime"
        self.event_path = self.layout.raw_dir / f"{self.run_id}.ndjson"
        self.state_path = self.layout.export_dir / f"{self.run_id}.state.json"
        self.config_path: Path | None = None
        self.leaderboard_path: Path | None = None
        self.event_counts: dict[str, int] = {}
        self._event_index = 0
        self._attempt_counters: dict[str, int] = {}
        self._sessions: dict[str, dict[str, Any]] = {}
        self._lock = Lock()
        self._write_state("initialized")

    def _resolve_source_commit(self) -> str:
        override = os.getenv("DEXTER_SOURCE_COMMIT")
        if override:
            return override

        try:
            return (
                subprocess.check_output(
                    ["git", "rev-parse", "HEAD"],
                    cwd=self.source_repo_root,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                .strip()
            )
        except Exception:
            return "unknown"

    def _write_state(self, status: str) -> None:
        write_json(
            self.state_path,
            {
                "run_id": self.run_id,
                "status": status,
                "source_system": "dexter",
                "source_commit": self.source_commit,
                "mode": self.mode,
                "transport_mode": self.transport_mode,
                "host_role": self.host_role,
                "started_at_utc": isoformat_utc(self.started_at),
                "ended_at_utc": isoformat_utc(self.ended_at) if self.ended_at else None,
                "paths": {
                    **self.layout.metadata(),
                    "raw_events_file": str(self.event_path),
                    "config_snapshot": str(self.config_path) if self.config_path else None,
                    "leaderboard_snapshot": str(self.leaderboard_path) if self.leaderboard_path else None,
                },
                "event_counts": self.event_counts,
            },
        )

    def emit(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        session_id: str | None = None,
        mint: str | None = None,
        creator: str | None = None,
        ts: datetime | None = None,
    ) -> str | None:
        try:
            event_time = ts or utc_now()
            with self._lock:
                self._event_index += 1
                self.event_counts[event_type] = self.event_counts.get(event_type, 0) + 1
                event = {
                    "run_id": self.run_id,
                    "event_id": f"{self.run_id}-{self._event_index:06d}",
                    "event_type": event_type,
                    "ts_utc": isoformat_utc(event_time),
                    "source_system": "dexter",
                    "source_commit": self.source_commit,
                    "mode": self.mode,
                    "transport_mode": self.transport_mode,
                    "payload": payload,
                }
                if session_id:
                    event["session_id"] = session_id
                if mint:
                    event["mint"] = mint
                if creator:
                    event["creator"] = creator

                self.event_path.parent.mkdir(parents=True, exist_ok=True)
                with self.event_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(event, default=json_default))
                    handle.write("\n")
            return event["event_id"]
        except Exception as exc:
            LOGGER.warning("best-effort instrumentation failed for %s: %s", event_type, exc)
            return None
        finally:
            self._write_state("running")

    def export_masked_config(self, settings_payload: dict[str, Any]) -> Path:
        self.config_path = self.layout.config_dir / f"{self.run_id}.config.json"
        write_json(
            self.config_path,
            {
                "run_id": self.run_id,
                "captured_at_utc": isoformat_utc(),
                "source_commit": self.source_commit,
                "paths": self.layout.metadata(),
                "settings": settings_payload,
                "environment": {
                    "HTTP_URL": mask_secret(os.getenv("HTTP_URL")),
                    "WS_URL": mask_secret(os.getenv("WS_URL")),
                    "PRIVATE_KEY": mask_secret(os.getenv("PRIVATE_KEY")),
                },
            },
        )
        self._write_state("config_exported")
        return self.config_path

    def _trust_level_projection(self, creator_data: dict[str, Any]) -> int:
        mint_count = int(creator_data.get("mint_count", 0) or 0)
        median_peak_market_cap = as_float(creator_data.get("median_peak_market_cap", 0))
        if mint_count == 1:
            return 1
        if median_peak_market_cap >= 50000:
            return 2
        if median_peak_market_cap >= 0:
            return 1
        return 0

    def export_leaderboard(self, leaderboard: list[tuple[str, dict[str, Any]]]) -> Path:
        self.leaderboard_path = self.layout.export_dir / f"{self.run_id}.leaderboard.json"
        snapshot_entries = []
        cohort_size = len(leaderboard)
        for creator, entry in leaderboard:
            projected_trust_level = self._trust_level_projection(entry)
            snapshot_entries.append(
                {
                    "creator": creator,
                    "projected_trust_level": projected_trust_level,
                    **entry,
                }
            )
            self.emit(
                "creator_candidate",
                {
                    "candidate_source": "dexter_leaderboard",
                    "score_components": {
                        "performance_score": as_float(entry.get("performance_score", 0)),
                        "trust_factor": as_float(entry.get("trust_factor", 0)),
                        "median_success_ratio": as_float(entry.get("median_success_ratio", 0)),
                        "mint_count": int(entry.get("mint_count", 0) or 0),
                        "total_swaps": int(entry.get("total_swaps", 0) or 0),
                        "median_peak_market_cap": as_float(entry.get("median_peak_market_cap", 0)),
                    },
                    "score_total": as_float(entry.get("performance_score", 0)),
                    "cohort_size": cohort_size,
                    "reason": "leaderboard_admission",
                    "projected_trust_level": projected_trust_level,
                },
                creator=creator,
            )

        write_json(
            self.leaderboard_path,
            {
                "run_id": self.run_id,
                "captured_at_utc": isoformat_utc(),
                "source_commit": self.source_commit,
                "cohort_size": cohort_size,
                "entries": snapshot_entries,
            },
        )
        self._write_state("leaderboard_exported")
        return self.leaderboard_path

    def _ensure_session(self, mint_id: str, creator: str | None = None, **extra: Any) -> dict[str, Any]:
        if mint_id not in self._sessions:
            self._sessions[mint_id] = {
                "session_id": f"{mint_id}-{int(self.started_at.timestamp() * 1000)}",
                "creator": creator,
                "mint": mint_id,
                "observed_at": utc_now(),
            }
        session = self._sessions[mint_id]
        if creator:
            session["creator"] = creator
        session.update(extra)
        return session

    def observe_mint(
        self,
        mint_id: str,
        creator: str,
        *,
        bonding_curve: str | None,
        open_price: Any,
        slot: int | None,
        mint_sig: str | None,
        name: str | None = None,
    ) -> str:
        session = self._ensure_session(
            mint_id,
            creator=creator,
            bonding_curve=bonding_curve,
            observed_open_price=as_float(open_price),
        )
        if not session.get("mint_observed_emitted"):
            self.emit(
                "mint_observed",
                {
                    "market": "pump_fun",
                    "pool_id": bonding_curve or mint_id,
                    "first_seen_slot": int(slot or 0),
                    "open_price": as_float(open_price),
                    "is_migrated": False,
                    "mint_sig": mint_sig or "",
                    "name": name or "",
                },
                session_id=session["session_id"],
                mint=mint_id,
                creator=creator,
            )
            session["mint_observed_emitted"] = True
        return session["session_id"]

    def record_candidate_rejected(
        self,
        mint_id: str,
        creator: str,
        *,
        reject_reason: str,
        gate_name: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        payload = {
            "candidate_source": "dexter_leaderboard_match",
            "reject_reason": reject_reason,
            "gate_name": gate_name,
        }
        if details:
            payload.update(details)
        self.emit(
            "candidate_rejected",
            payload,
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
        )

    def record_entry_signal(
        self,
        mint_id: str,
        creator: str,
        *,
        signal_price: Any,
        trust_level: int,
        bonding_curve: str | None,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator, bonding_curve=bonding_curve)
        signal_time = utc_now()
        signal_latency_ms = int((signal_time - session["observed_at"]).total_seconds() * 1000)
        session.update(
            signal_at=signal_time,
            signal_price=as_float(signal_price),
            trust_level=trust_level,
        )
        self.emit(
            "entry_signal",
            {
                "candidate_source": "dexter_leaderboard_match",
                "market": "pump_fun",
                "signal_reason": "matched_leaderboard_creator",
                "signal_price": as_float(signal_price),
                "signal_slot": 0,
                "signal_latency_ms": max(signal_latency_ms, 0),
                "trust_level": trust_level,
                "pool_id": bonding_curve or mint_id,
            },
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
            ts=signal_time,
        )

    def record_entry_attempt(
        self,
        mint_id: str,
        creator: str,
        *,
        quote_price: Any,
        expected_tokens_out: Any,
        slippage_bps: int,
        wallet_available_before: int,
        reserved_balance_before: int,
        reserved_cost: int,
        tx_strategy: str,
        route: str = "pump_fun",
    ) -> int:
        session = self._ensure_session(mint_id, creator=creator)
        attempt_index = self._attempt_counters.get(mint_id, 0) + 1
        self._attempt_counters[mint_id] = attempt_index
        attempts = session.setdefault("attempts", {})
        attempts[attempt_index] = {
            "started_at": utc_now(),
            "quote_price": as_float(quote_price),
            "wallet_available_before": wallet_available_before,
        }
        self.emit(
            "entry_attempt",
            {
                "attempt_index": attempt_index,
                "market": "pump_fun",
                "route": route,
                "quote_price": as_float(quote_price),
                "expected_tokens_out": as_float(expected_tokens_out),
                "slippage_bps": int(slippage_bps),
                "wallet_available_before": int(wallet_available_before),
                "tx_strategy": tx_strategy,
                "reserved_balance_before": int(reserved_balance_before),
                "reserved_cost": int(reserved_cost),
            },
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
        )
        return attempt_index

    def record_entry_rejected(
        self,
        mint_id: str,
        creator: str,
        *,
        reject_reason: str,
        tx_signature: str = "",
        attempt_index: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        resolved_attempt = attempt_index or max(self._attempt_counters.get(mint_id, 1), 1)
        payload = {
            "attempt_index": resolved_attempt,
            "reject_reason": reject_reason,
            "tx_signature": tx_signature,
        }
        if details:
            payload.update(details)
        self.emit(
            "entry_rejected",
            payload,
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
        )

    def record_entry_fill(
        self,
        mint_id: str,
        creator: str,
        *,
        fill_price: Any,
        fill_qty: Any,
        tx_signature: str,
        wallet_balance_after: int,
        confirmation_path: str,
        attempt_index: int | None = None,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        resolved_attempt = attempt_index or max(self._attempt_counters.get(mint_id, 1), 1)
        attempt = session.get("attempts", {}).get(resolved_attempt, {})
        fill_time = utc_now()
        started_at = attempt.get("started_at") or session.get("signal_at") or fill_time
        fill_latency_ms = int((fill_time - started_at).total_seconds() * 1000)
        fill_price_value = as_float(fill_price)
        session.update(
            entry_fill_at=fill_time,
            entry_fill_price=fill_price_value,
            token_balance=fill_qty,
            wallet_balance_after=wallet_balance_after,
            confirmation_path=confirmation_path,
        )
        if session.get("peak_price") is None:
            session["peak_price"] = fill_price_value
            session["peak_price_at"] = fill_time
        if session.get("low_price") is None:
            session["low_price"] = fill_price_value

        self.emit(
            "entry_fill",
            {
                "attempt_index": resolved_attempt,
                "fill_price": fill_price_value,
                "fill_qty": as_float(fill_qty),
                "fill_latency_ms": max(fill_latency_ms, 0),
                "tx_signature": tx_signature,
                "wallet_balance_after": int(wallet_balance_after),
                "confirmation_path": confirmation_path,
            },
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
            ts=fill_time,
        )

    def record_session_update(
        self,
        mint_id: str,
        creator: str,
        *,
        price: Any,
        highest_price: Any,
        buys: int,
        sells: int,
        liquidity: Any,
        composite_score: Any,
        current_target_pct: Any,
        state_reason: str,
        creator_token_amount: Any = 0,
        creator_sold: bool = False,
        txns_in_zero: int | None = None,
        txns_in_n: int | None = None,
        malicious: bool = False,
        drop_time: bool = False,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        now = utc_now()
        price_value = as_float(price)
        high_value = max(as_float(highest_price), price_value)
        if session.get("peak_price") is None or high_value >= session.get("peak_price", 0):
            session["peak_price"] = high_value
            session["peak_price_at"] = now
        if session.get("low_price") is None or price_value <= session.get("low_price", price_value):
            session["low_price"] = price_value

        basis = session.get("entry_fill_price") or session.get("signal_price") or session.get("observed_open_price")
        mfe_pct = pct_change(basis, session.get("peak_price"))
        mae_pct = pct_change(basis, session.get("low_price"))

        payload = {
            "price": price_value,
            "highest_price": high_value,
            "buys": int(buys),
            "sells": int(sells),
            "liquidity": as_float(liquidity),
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "state_reason": state_reason,
            "creator_token_amount": as_float(creator_token_amount),
            "creator_sold": creator_sold,
            "txns_in_zero": txns_in_zero,
            "txns_in_n": txns_in_n,
            "composite_score": as_float(composite_score),
            "current_target_pct": as_float(current_target_pct),
            "malicious": malicious,
            "drop_time": drop_time,
        }
        self.emit(
            "session_update",
            payload,
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
            ts=now,
        )

    def record_exit_signal(
        self,
        mint_id: str,
        creator: str,
        *,
        exit_reason: str,
        signal_price: Any,
        theoretical_best_price: Any,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        signal_time = utc_now()
        session.update(
            exit_reason=exit_reason,
            exit_signal_at=signal_time,
            exit_signal_price=as_float(signal_price),
        )
        payload = {
            "exit_reason": exit_reason,
            "signal_price": as_float(signal_price),
            "theoretical_best_price": as_float(theoretical_best_price),
            "realized_vs_peak_gap_pct": pct_change(signal_price, theoretical_best_price),
        }
        self.emit(
            "exit_signal",
            payload,
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
            ts=signal_time,
        )

    def record_exit_fill(
        self,
        mint_id: str,
        creator: str,
        *,
        fill_price: Any,
        fill_qty: Any,
        tx_signature: str,
        exit_reason: str,
        wallet_balance_after: int,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        fill_time = utc_now()
        exit_signal_at = session.get("exit_signal_at") or fill_time
        fill_latency_ms = int((fill_time - exit_signal_at).total_seconds() * 1000)
        session.update(
            exit_fill_at=fill_time,
            exit_fill_price=as_float(fill_price),
            wallet_balance_after=wallet_balance_after,
        )
        self.emit(
            "exit_fill",
            {
                "fill_price": as_float(fill_price),
                "fill_qty": as_float(fill_qty),
                "fill_latency_ms": max(fill_latency_ms, 0),
                "tx_signature": tx_signature,
                "exit_reason": exit_reason,
                "wallet_balance_after": int(wallet_balance_after),
            },
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
            ts=fill_time,
        )

    def record_position_closed(
        self,
        mint_id: str,
        creator: str,
        *,
        stale_position_flag: bool,
    ) -> None:
        session = self._ensure_session(mint_id, creator=creator)
        entry_price = session.get("entry_fill_price") or session.get("signal_price") or 0
        exit_price = session.get("exit_fill_price") or session.get("exit_signal_price") or 0
        peak_price = session.get("peak_price") or entry_price
        low_price = session.get("low_price") or entry_price
        fill_time = session.get("entry_fill_at") or session.get("signal_at") or utc_now()
        closed_time = session.get("exit_fill_at") or utc_now()
        peak_time = session.get("peak_price_at") or fill_time

        self.emit(
            "position_closed",
            {
                "entry_price": as_float(entry_price),
                "exit_price": as_float(exit_price),
                "realized_return_pct": pct_change(entry_price, exit_price),
                "mfe_pct": pct_change(entry_price, peak_price),
                "mae_pct": pct_change(entry_price, low_price),
                "time_to_peak_ms": int((peak_time - fill_time).total_seconds() * 1000),
                "session_duration_ms": int((closed_time - fill_time).total_seconds() * 1000),
                "stale_position_flag": stale_position_flag,
            },
            session_id=session["session_id"],
            mint=mint_id,
            creator=creator,
            ts=closed_time,
        )
        session["closed"] = True

    def export_replay_record(self, mint_id: str, payload: dict[str, Any]) -> Path:
        exporter = ReplayExporter(
            output_root=self.layout.output_root,
            runtime_root=self.layout.runtime_root,
        )
        return exporter.export_record(mint_id, payload)

    def finalize(self) -> None:
        if self.ended_at is not None:
            return
        self.ended_at = utc_now()
        event_count_before_summary = sum(self.event_counts.values())
        self.emit(
            "run_summary",
            {
                "candidate_count": self.event_counts.get("creator_candidate", 0),
                "entry_attempt_count": self.event_counts.get("entry_attempt", 0),
                "entry_fill_count": self.event_counts.get("entry_fill", 0),
                "position_closed_count": self.event_counts.get("position_closed", 0),
                "event_count": event_count_before_summary + 1,
                "replayable": bool(self.config_path and self.leaderboard_path),
            },
            ts=self.ended_at,
        )
        self._write_state("completed")


async def export_stagnant_mints(
    db_dsn: str,
    *,
    output_root: str | os.PathLike[str] | None = None,
    runtime_root: str | None = None,
) -> int:
    import asyncpg

    exporter = ReplayExporter(output_root=output_root, runtime_root=runtime_root)
    conn = await asyncpg.connect(db_dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT mint_id, name, symbol, owner, holders, price_history, tx_counts, volume,
                   peak_price_change, peak_market_cap, final_market_cap, final_ohlc, mint_sig,
                   bonding_curve, slot_delay, timestamp
            FROM stagnant_mints
            ORDER BY timestamp DESC
            """
        )
    finally:
        await conn.close()

    for row in rows:
        exporter.export_record(row["mint_id"], dict(row))

    return len(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dexter Vexter replay exporter")
    parser.add_argument("--db-dsn", default="postgres://dexter_user:admin123@127.0.0.1/dexter_db")
    parser.add_argument("--output-root")
    parser.add_argument("--runtime-root")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exported = asyncio.run(
        export_stagnant_mints(
            args.db_dsn,
            output_root=args.output_root,
            runtime_root=args.runtime_root,
        )
    )
    print(f"exported {exported} stagnant mint replay records")


if __name__ == "__main__":
    main()
