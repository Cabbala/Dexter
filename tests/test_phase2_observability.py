import asyncio
import datetime as dt
import importlib.util
import sys
import types
from pathlib import Path

from dexter_time import normalize_unix_timestamp, safe_utc_datetime_from_timestamp


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _ColorStub:
    def __getattr__(self, name):
        return ""


class _ReplayExporter:
    def export_record(self, *args, **kwargs):
        return None


def install_market_stubs():
    common_module = types.ModuleType("common_")
    common_module.PUMP_FUN = "pump"
    common_module.WS_URL = "wss://ws.example.com"

    colors_module = types.ModuleType("colors")
    colors_module.cc = _ColorStub()

    instrumentation_module = types.ModuleType("instrumentation")
    instrumentation_module.ReplayExporter = _ReplayExporter

    dexlab_common_module = types.ModuleType("DexLab.common_")
    dexlab_common_module.PUMP_FUN = "pump"
    dexlab_common_module.WS_URL = "wss://ws.example.com"

    dexlab_colors_module = types.ModuleType("DexLab.colors")
    dexlab_colors_module.cc = _ColorStub()

    dexlab_instrumentation_module = types.ModuleType("DexLab.instrumentation")
    dexlab_instrumentation_module.ReplayExporter = _ReplayExporter

    sys.modules.update(
        {
            "common_": common_module,
            "colors": colors_module,
            "instrumentation": instrumentation_module,
            "DexLab.common_": dexlab_common_module,
            "DexLab.colors": dexlab_colors_module,
            "DexLab.instrumentation": dexlab_instrumentation_module,
        }
    )


install_market_stubs()

MARKET_MODULE_PATH = REPO_ROOT / "DexLab" / "market.py"
MARKET_SPEC = importlib.util.spec_from_file_location("dexter_market_phase2_test", MARKET_MODULE_PATH)
market_module = importlib.util.module_from_spec(MARKET_SPEC)
assert MARKET_SPEC and MARKET_SPEC.loader
MARKET_SPEC.loader.exec_module(market_module)

Market = market_module.Market


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, tables=None):
        self.executed = []
        self.tables = tables or [
            {"tablename": "mints"},
            {"tablename": "stagnant_mints"},
        ]

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def fetch(self, query):
        return self.tables


class FakePool:
    def __init__(self, conn):
        self.conn = conn
        self.closed = False

    def acquire(self):
        return _Acquire(self.conn)

    async def close(self):
        self.closed = True


class FakePhase2Store:
    def __init__(self):
        self.bound_pool = None
        self.ensure_schema_calls = 0
        self.raw_events = []
        self.mint_snapshots = []

    def bind_pool(self, pool):
        self.bound_pool = pool

    async def ensure_schema(self):
        self.ensure_schema_calls += 1

    async def record_raw_event(self, **kwargs):
        self.raw_events.append(kwargs)
        return "phase2-fingerprint"

    async def record_mint_snapshot(self, **kwargs):
        self.mint_snapshots.append(kwargs)


def test_normalize_unix_timestamp_accepts_milliseconds_and_clamps_future_values():
    now = 1_710_000_000

    assert normalize_unix_timestamp(1_710_000_000_123, now=now, fallback=0) == now
    assert normalize_unix_timestamp(now + 999_999, now=now, fallback=1234) == 1234

    observed_at = safe_utc_datetime_from_timestamp(1_710_000_000_123, now=now, fallback=0)
    assert observed_at == dt.datetime.fromtimestamp(now, tz=dt.timezone.utc)


def test_market_record_raw_event_normalizes_payload_timestamp():
    phase2 = FakePhase2Store()
    market = Market.__new__(Market)
    market.phase2 = phase2

    fingerprint = asyncio.run(
        market.record_raw_event(
            "pump",
            "sig-1",
            42,
            3,
            "swaps",
            False,
            {
                "mint": "mint-1",
                "user": "owner-1",
                "timestamp": 1_710_000_000_123,
            },
            raw_logs=["Program data: vdtpayload"],
        )
    )

    assert fingerprint == "phase2-fingerprint"
    assert phase2.raw_events
    payload = phase2.raw_events[0]["payload"]
    assert payload["timestamp"] == 1_710_000_000
    assert phase2.raw_events[0]["observed_at"] == dt.datetime.fromtimestamp(1_710_000_000, tz=dt.timezone.utc)


def test_store_mint_records_phase2_snapshot_without_changing_primary_insert():
    conn = FakeConn()
    phase2 = FakePhase2Store()
    market = Market.__new__(Market)
    market.stop_event = asyncio.Event()
    market.db_pool = FakePool(conn)
    market.phase2 = phase2

    asyncio.run(
        market.store_mint(
            "mint-1",
            {
                "info": {"name": "Mint One", "symbol": "M1"},
                "owner": "creator-1",
                "market_cap": 0,
                "price_history": {},
                "price_usd": 0,
                "liquidity": 0,
                "high_price": 0,
                "low_price": 0,
                "open_price": 0,
                "current_price": 0,
                "age": 0,
                "tx_counts": {"swaps": 0, "buys": 0, "sells": 0},
                "holders": {},
                "mint_sig": "mint-sig",
                "bonding_curve": "curve-1",
                "created": 1_710_000_000,
            },
            last_event_fingerprint="phase2-fingerprint",
            last_event_slot=42,
        )
    )

    assert len(conn.executed) == 1
    assert phase2.mint_snapshots
    snapshot = phase2.mint_snapshots[0]
    assert snapshot["mint_id"] == "mint-1"
    assert snapshot["lifecycle_state"] == "active"
    assert snapshot["last_event_fingerprint"] == "phase2-fingerprint"
    assert snapshot["last_event_slot"] == 42
    assert snapshot["snapshot"]["bonding_curve"] == "curve-1"


def test_init_db_disables_phase2_capture_if_schema_bootstrap_fails():
    conn = FakeConn()
    pool = FakePool(conn)

    class FailingPhase2Store(FakePhase2Store):
        async def ensure_schema(self):
            raise RuntimeError("schema bootstrap failed")

    market = Market.__new__(Market)
    market.db_dsn = "postgres://dexter"
    market.phase2 = FailingPhase2Store()

    original_create_pool = market_module.asyncpg.create_pool

    async def fake_create_pool(*args, **kwargs):
        return pool

    market_module.asyncpg.create_pool = fake_create_pool
    try:
        asyncio.run(market.init_db())
    finally:
        market_module.asyncpg.create_pool = original_create_pool

    assert market.db_pool is pool
    assert market.phase2 is None
