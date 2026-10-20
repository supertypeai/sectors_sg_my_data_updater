import os
import sys
from pathlib import Path

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.dummy")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sgx_periodic_financial"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sgx_scrape"))

import financial_guards
import sgx_financials_annual_scraper as annual

INCOME = {
    "pretax_income": 100_000_000.0,
    "income_taxes": 20_000_000.0,
    "net_income": 60_000_000.0,
}


def _tax_bridge(metrics):
    return [i for i in financial_guards.check(metrics) if i["check"] == "tax_bridge"]


def test_tax_bridge_uses_after_tax_income_when_present():
    metrics = {"income_statement": {**INCOME, "net_income_after_taxes": 80_000_000.0}}
    assert _tax_bridge(metrics) == []


def test_tax_bridge_falls_back_to_net_income():
    assert len(_tax_bridge({"income_statement": INCOME})) == 1


def test_annual_income_maps_net_income_after_taxes():
    out = annual.transform_metrics(
        {"netIncome": 60_000_000, "netIncomeAfterTaxes": 80_000_000}, "income"
    )
    assert out["net_income"] == 60_000_000
    assert out["net_income_after_taxes"] == 80_000_000
