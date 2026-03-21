import asyncio
import importlib.util
import sys
import types
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _ColorStub:
    def __getattr__(self, name):
        return ""


def install_dexter_stubs():
    dexlab_pkg = types.ModuleType("DexLab")
    colors_module = types.ModuleType("DexLab.colors")
    colors_module.cc = _ColorStub()

    common_module = types.ModuleType("DexLab.common_")
    common_module.PUMP_FUN = "pump"
    common_module.RPC_URL = "https://rpc.example.com"
    common_module.WS_URL = "wss://ws.example.com"
    common_module.PRIV_KEY = "paper-private-key"
    common_module.STAKED_API = "https://rpc.example.com"

    wslogs_module = types.ModuleType("DexLab.wsLogs")
    wslogs_module.DexBetterLogs = object

    pump_fun_module = types.ModuleType("DexLab.pump_fun")
    pump_fun_module.PumpFun = object

    swaps_module = types.ModuleType("DexLab.swaps")
    swaps_module.SolanaSwaps = object

    utils_module = types.ModuleType("DexLab.utils")

    async def _usd_to_lamports(amount, sol_price):
        return 0

    utils_module.usd_to_lamports = _usd_to_lamports
    utils_module.usd_to_microlamports = lambda amount, sol_price, compute_units: 0

    instrumentation_module = types.ModuleType("DexLab.instrumentation")
    instrumentation_module.DexterInstrumentation = object

    dexai_pkg = types.ModuleType("DexAI")
    trust_factor_module = types.ModuleType("DexAI.trust_factor")

    class Analyzer:
        def __init__(self, db_dsn):
            self.db_dsn = db_dsn
            self.sol_price_usd = Decimal("150")

    trust_factor_module.Analyzer = Analyzer

    aiohttp_module = types.ModuleType("aiohttp")

    class ClientSession:
        pass

    aiohttp_module.ClientSession = ClientSession

    asyncpg_module = types.ModuleType("asyncpg")
    asyncpg_module.create_pool = object

    base58_module = types.ModuleType("base58")
    base58_module.b58decode = lambda value: b"stub-private-key"

    solders_module = types.ModuleType("solders")
    solders_keypair_module = types.ModuleType("solders.keypair")

    class Keypair:
        @classmethod
        def from_bytes(cls, value):
            return cls()

        def pubkey(self):
            return "paper-wallet"

    solders_keypair_module.Keypair = Keypair

    websockets_module = types.ModuleType("websockets")
    websockets_module.exceptions = SimpleNamespace(ConnectionClosedError=Exception)

    settings_module = types.ModuleType("settings")
    settings_module.AMOUNT_BUY_TL_1 = Decimal("0.01")
    settings_module.AMOUNT_BUY_TL_2 = Decimal("0.02")
    settings_module.BUY_FEE = Decimal("0.1")
    settings_module.SELL_FEE = Decimal("0.1")
    settings_module.SLIPPAGE_AMOUNT = Decimal("1.3")
    settings_module.PRICE_STEP_UNITS = Decimal("10")
    settings_module.PROFIT_MARGIN = Decimal("1")
    settings_module.INCREMENT_THRESHOLD = Decimal("25")
    settings_module.DECREMENT_THRESHOLD = Decimal("10")
    settings_module.DROP_TIME = 30
    settings_module.STAGNANT_UNDER_PRICE = 13
    settings_module.LEADERBOARD_UPDATE_INTERVAL = 15
    settings_module.INCREMENT_COOLDOWN = 1
    settings_module.HTTP_URL = "https://rpc.example.com"
    settings_module.WS_URL = "wss://ws.example.com"
    settings_module.PRIV_KEY = "paper-private-key"
    settings_module.DB_DSN = "postgres://dexter"

    solana_module = types.ModuleType("solana")
    solana_rpc_module = types.ModuleType("solana.rpc")
    solana_async_api_module = types.ModuleType("solana.rpc.async_api")

    class AsyncClient:
        def __init__(self, endpoint):
            self.endpoint = endpoint

    solana_async_api_module.AsyncClient = AsyncClient

    dotenv_module = types.ModuleType("dotenv")
    dotenv_module.load_dotenv = lambda *args, **kwargs: None

    sys.modules.update(
        {
            "DexLab": dexlab_pkg,
            "DexLab.colors": colors_module,
            "DexLab.common_": common_module,
            "DexLab.wsLogs": wslogs_module,
            "DexLab.pump_fun": pump_fun_module,
            "DexLab.swaps": swaps_module,
            "DexLab.utils": utils_module,
            "DexLab.instrumentation": instrumentation_module,
            "DexAI": dexai_pkg,
            "DexAI.trust_factor": trust_factor_module,
            "aiohttp": aiohttp_module,
            "asyncpg": asyncpg_module,
            "base58": base58_module,
            "solders": solders_module,
            "solders.keypair": solders_keypair_module,
            "websockets": websockets_module,
            "settings": settings_module,
            "solana": solana_module,
            "solana.rpc": solana_rpc_module,
            "solana.rpc.async_api": solana_async_api_module,
            "dotenv": dotenv_module,
        }
    )


install_dexter_stubs()

MODULE_PATH = REPO_ROOT / "Dexter.py"
SPEC = importlib.util.spec_from_file_location("dexter_runtime", MODULE_PATH)
dexter_module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(dexter_module)

Dexter = dexter_module.Dexter


class FakeInstrumentation:
    def __init__(self):
        self.entry_attempts = []
        self.entry_rejections = []
        self.exit_fills = []
        self.position_closures = []

    def record_entry_attempt(self, *args, **kwargs):
        self.entry_attempts.append(kwargs)
        return len(self.entry_attempts)

    def record_entry_rejected(self, *args, **kwargs):
        self.entry_rejections.append(kwargs)

    def record_exit_fill(self, *args, **kwargs):
        self.exit_fills.append(kwargs)

    def record_position_closed(self, *args, **kwargs):
        self.position_closures.append(kwargs)


class FakePumpSwap:
    def __init__(self):
        self.live_buy_called = 0
        self.live_sell_called = 0

    async def paper_buy_quote(self, *args, **kwargs):
        return {
            "fill_qty": 2_500_000,
            "fill_price": Decimal("0.00012"),
            "lamports_spent": 1_000,
        }

    async def paper_sell_quote(self, *args, **kwargs):
        return {
            "fill_price": Decimal("0.00018"),
            "lamports_out": 250_000,
        }

    async def pump_buy(self, *args, **kwargs):
        self.live_buy_called += 1
        raise AssertionError("paper_live should not call the live buy sender")

    async def pump_sell(self, *args, **kwargs):
        self.live_sell_called += 1
        raise AssertionError("paper_live should not call the live sell sender")


def build_paper_dexter():
    dexter = Dexter.__new__(Dexter)
    dexter.execution_mode = "paper_live"
    dexter.instrumentation = FakeInstrumentation()
    dexter.analyzer = SimpleNamespace(sol_price_usd=Decimal("150"))
    dexter.wallet_balance = 0
    dexter.paper_wallet_balance = 1_000_000_000
    dexter.pending_buy_spend = {}
    dexter.paper_positions = {}
    dexter.paper_pending_fills = {}
    dexter.holdings = {}
    dexter.swap_folder = {
        "mint-1": {
            "bonding_curve": "curve-1",
            "state": {"price": Decimal("0.00012")},
        }
    }
    dexter.time_start = 0
    dexter.pump_swap = FakePumpSwap()

    async def save_result(payload):
        return payload

    async def validate_result(owner, reason):
        return owner, reason

    dexter.save_result = save_result
    dexter._validate_result = validate_result
    return dexter


def test_buy_paper_live_creates_synthetic_fill_without_live_send(monkeypatch):
    dexter = build_paper_dexter()

    async def fake_usd_to_lamports(amount, sol_price):
        return 1_000

    monkeypatch.setattr(dexter_module, "usd_to_lamports", fake_usd_to_lamports)
    monkeypatch.setattr(dexter_module, "usd_to_microlamports", lambda amount, sol_price, compute_units: 10_000)

    result = asyncio.run(dexter.buy("mint-1", 1, "creator-1"))

    assert result == "paper-entry:mint-1:1"
    assert dexter.paper_wallet_balance == 999_998_999
    assert dexter.instrumentation.entry_attempts[0]["tx_strategy"] == "paper_quote"
    assert dexter.paper_pending_fills["mint-1"]["confirmation_path"] == "paper_fill"
    assert dexter.paper_pending_fills["mint-1"]["fill_qty"] == 2_500_000
    assert dexter.pump_swap.live_buy_called == 0


def test_sell_paper_live_updates_paper_balance_without_live_send(monkeypatch):
    dexter = build_paper_dexter()
    dexter.holdings["mint-1"] = {"mode": "paper_live"}
    dexter.paper_positions["mint-1"] = {"fill_qty": 2_500_000}
    dexter.paper_wallet_balance = 500_000_000

    monkeypatch.setattr(dexter_module, "usd_to_microlamports", lambda amount, sol_price, compute_units: 10_000)

    result = asyncio.run(
        dexter.sell(
            "mint-1",
            2_500_000,
            "safe",
            "creator-1",
            1,
            Decimal("0.00012"),
        )
    )

    assert result == "paper-exit:mint-1"
    assert dexter.paper_wallet_balance == 500_250_000
    assert dexter.instrumentation.exit_fills[0]["confirmation_path"] == "paper_fill"
    assert dexter.instrumentation.exit_fills[0]["tx_strategy"] == "paper_quote"
    assert dexter.instrumentation.position_closures
    assert "mint-1" not in dexter.holdings
    assert dexter.pump_swap.live_sell_called == 0
