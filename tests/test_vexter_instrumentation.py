import json
import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "DexLab" / "instrumentation.py"
SPEC = importlib.util.spec_from_file_location("dexter_instrumentation", MODULE_PATH)
instrumentation = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(instrumentation)

DexterInstrumentation = instrumentation.DexterInstrumentation
ReplayExporter = instrumentation.ReplayExporter


def test_instrumentation_writes_expected_artifacts(monkeypatch, tmp_path):
    monkeypatch.setenv("VEXTER_OUTPUT_ROOT", str(tmp_path))
    monkeypatch.setenv("VEXTER_RUNTIME_ROOT", r"C:\Users\bot\quant\Vexter")
    monkeypatch.setenv("VEXTER_RUN_ID", "dexter-test-run")
    monkeypatch.setenv("DEXTER_SOURCE_COMMIT", "deadbeef")
    monkeypatch.setenv("PRIVATE_KEY", "super-secret-private-key")
    monkeypatch.setenv("HTTP_URL", "https://rpc.example.com")
    monkeypatch.setenv("WS_URL", "wss://ws.example.com")

    writer = DexterInstrumentation(source_repo_root=tmp_path)
    writer.export_masked_config({"buy_amount_tl_1": 0.01, "buy_amount_tl_2": 0.02})
    writer.export_leaderboard(
        [
            (
                "creator-1",
                {
                    "mint_count": 2,
                    "performance_score": 123.4,
                    "trust_factor": 0.8,
                    "median_success_ratio": 55.0,
                    "total_swaps": 123,
                    "median_peak_market_cap": 65000.0,
                },
            )
        ]
    )
    writer.observe_mint(
        "mint-1",
        "creator-1",
        bonding_curve="curve-1",
        open_price=0.00012,
        slot=42,
        mint_sig="mint-sig",
        name="Mint One",
    )
    writer.record_entry_signal(
        "mint-1",
        "creator-1",
        signal_price=0.00013,
        trust_level=2,
        bonding_curve="curve-1",
    )
    writer.record_entry_attempt(
        "mint-1",
        "creator-1",
        quote_price=0.00013,
        expected_tokens_out=1000,
        slippage_bps=13000,
        wallet_available_before=1000000,
        reserved_balance_before=0,
        reserved_cost=12345,
        tx_strategy="pump_buy_priority_fee",
    )
    writer.record_entry_fill(
        "mint-1",
        "creator-1",
        fill_price=0.00014,
        fill_qty=950,
        tx_signature="buy-tx",
        wallet_balance_after=990000,
        confirmation_path="holder_balance",
    )
    writer.record_session_update(
        "mint-1",
        "creator-1",
        price=0.00016,
        highest_price=0.00018,
        buys=10,
        sells=2,
        liquidity=2500,
        composite_score=42,
        current_target_pct=40,
        state_reason="safe",
    )
    writer.record_exit_signal(
        "mint-1",
        "creator-1",
        exit_reason="safe",
        signal_price=0.00017,
        theoretical_best_price=0.00018,
    )
    writer.record_exit_fill(
        "mint-1",
        "creator-1",
        fill_price=0.00017,
        fill_qty=950,
        tx_signature="sell-tx",
        exit_reason="safe",
        wallet_balance_after=1010000,
        confirmation_path="paper_fill",
        tx_strategy="paper_quote",
    )
    writer.record_position_closed("mint-1", "creator-1", stale_position_flag=False)
    writer.finalize()

    config_path = tmp_path / "runtime" / "dexter" / "config" / "dexter-test-run.config.json"
    leaderboard_path = tmp_path / "runtime" / "dexter" / "export" / "dexter-test-run.leaderboard.json"
    event_path = tmp_path / "data" / "raw" / "dexter" / "dexter-test-run.ndjson"

    assert config_path.exists()
    assert leaderboard_path.exists()
    assert event_path.exists()

    config_payload = json.loads(config_path.read_text())
    assert config_payload["environment"]["PRIVATE_KEY"].startswith("supe...")

    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    event_types = [event["event_type"] for event in events]
    assert "creator_candidate" in event_types
    assert "mint_observed" in event_types
    assert "entry_fill" in event_types
    assert "position_closed" in event_types
    assert event_types[-1] == "run_summary"
    exit_fill_event = next(event for event in events if event["event_type"] == "exit_fill")
    assert exit_fill_event["payload"]["confirmation_path"] == "paper_fill"
    assert exit_fill_event["payload"]["tx_strategy"] == "paper_quote"


def test_replay_exporter_writes_stagnant_snapshot(monkeypatch, tmp_path):
    monkeypatch.setenv("VEXTER_OUTPUT_ROOT", str(tmp_path))
    exporter = ReplayExporter(output_root=tmp_path)

    export_path = exporter.export_record(
        "mint-2",
        {
            "kind": "stagnant_mint",
            "slot_delay": "12.5",
            "price_history": {"1.000": 0.1, "2.000": 0.2},
        },
    )

    assert export_path == tmp_path / "data" / "replays" / "dexter" / "stagnant" / "mint-2.json"
    assert export_path.exists()
    payload = json.loads(export_path.read_text())
    assert payload["mint_id"] == "mint-2"
    assert payload["payload"]["kind"] == "stagnant_mint"
