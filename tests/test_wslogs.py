import asyncio
import importlib.util
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class _ColorStub:
    def __getattr__(self, name):
        return ""


def install_wslogs_stubs():
    dexlab_pkg = types.ModuleType("DexLab")
    dexlab_pkg.__path__ = [str(REPO_ROOT / "DexLab")]

    colors_module = types.ModuleType("DexLab.colors")
    colors_module.cc = _ColorStub()

    common_module = types.ModuleType("DexLab.common_")
    common_module.PUMP_FUN = "pump"
    common_module.WS_URL = "wss://ws.example.com"

    market_module = types.ModuleType("DexLab.market")

    class Market:
        def __init__(self, *args, **kwargs):
            pass

    market_module.Market = Market

    serializers_module = types.ModuleType("DexLab.serializers")

    class Interpreters:
        pass

    serializers_module.Interpreters = Interpreters

    aiohttp_module = types.ModuleType("aiohttp")

    class ClientSession:
        pass

    aiohttp_module.ClientSession = ClientSession

    websockets_module = types.ModuleType("websockets")

    sys.modules.update(
        {
            "DexLab": dexlab_pkg,
            "DexLab.colors": colors_module,
            "DexLab.common_": common_module,
            "DexLab.market": market_module,
            "DexLab.serializers": serializers_module,
            "aiohttp": aiohttp_module,
            "websockets": websockets_module,
        }
    )


install_wslogs_stubs()

MODULE_PATH = REPO_ROOT / "DexLab" / "wsLogs.py"
SPEC = importlib.util.spec_from_file_location("DexLab.wsLogs", MODULE_PATH)
wslogs_module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(wslogs_module)

DexBetterLogs = wslogs_module.DexBetterLogs


def build_logs():
    logs = DexBetterLogs.__new__(DexBetterLogs)
    logs.serializer = types.SimpleNamespace(
        parse_pumpfun_creation=lambda payload: {
            "mint": "mint-1",
            "bonding_curve": "curve-1",
            "user": "creator-1",
            "name": "Mint One",
            "symbol": "M1",
            "uri": "https://example.com/mint-1",
        },
        parse_pumpfun_transaction=lambda payload: {
            "mint": "mint-1",
            "user": "trader-1",
            "sol_amount": 1_000,
            "token_amount": 2_500_000,
            "is_buy": True,
            "timestamp": 123,
            "virtual_sol_reserves": 1_000,
            "virtual_token_reserves": 2_000,
        },
    )
    return logs


def test_validate_treats_creation_payload_as_mint_without_initialize_mint():
    logs = build_logs()

    is_mint, program_data = asyncio.run(
        logs.validate(
            ["Program data: G3Kcreationpayload"],
            "sig-1",
            debug=False,
        )
    )

    assert is_mint is True
    assert program_data[0]["bonding_curve"] == "curve-1"


def test_validate_keeps_transaction_payload_non_mint_without_initialize_mint():
    logs = build_logs()

    is_mint, program_data = asyncio.run(
        logs.validate(
            ["Program data: vdttransactionpayload"],
            "sig-2",
            debug=False,
        )
    )

    assert is_mint is False
    assert program_data[0]["sol_amount"] == 1_000
