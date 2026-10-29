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
    assert "net_income_after_taxes_is_derived" not in out


def test_annual_income_derives_after_tax_income_when_missing():
    out = annual.transform_metrics(
        {"netIncomeBeforeTaxes": 100_000_000, "provisionForIncomeTaxes": 20_000_000}, "income"
    )
    assert out["net_income_after_taxes"] == 80_000_000
    assert "net_income_after_taxes_is_derived" not in out
    assert _tax_bridge({"income_statement": out}) == []


def test_annual_income_keeps_reported_after_tax_income():
    out = annual.transform_metrics(
        {"netIncomeBeforeTaxes": 100_000_000, "provisionForIncomeTaxes": 20_000_000,
         "netIncomeAfterTaxes": 75_000_000}, "income"
    )
    assert out["net_income_after_taxes"] == 75_000_000


def test_annual_cash_flow_maps_foreign_exchange_effects():
    out = annual.transform_metrics(
        {"cashFromOperatingAct": 100_000_000, "cashFromInvestingAct": -50_000_000,
         "cashFromFinancingAct": -40_000_000, "netChangeInCash": 8_000_000,
         "foreignExchangeEffects": 2_000_000}, "cf"
    )
    assert out["effect_of_exchange_rate_changes"] == 2_000_000
    assert financial_guards.check({"cash_flow": out}) == []


def test_cash_flow_accepts_fx_subtracted_but_not_added():
    base = {"operating_cash_flow": 100_000_000, "investing_cash_flow": -50_000_000,
            "financing_cash_flow": -40_000_000}
    minus = {"cash_flow": {**base, "net_cash_flow": 8_000_000,
                           "effect_of_exchange_rate_changes": 2_000_000}}
    assert financial_guards.check(minus) == []
    plus = {"cash_flow": {**base, "net_cash_flow": 12_000_000,
                          "effect_of_exchange_rate_changes": 2_000_000}}
    assert [i for i in financial_guards.check(plus) if i["check"] == "cash_flow_sums"]


def test_cash_flow_uses_absolute_tolerance_not_percentage():
    base = {"operating_cash_flow": 100_000_000, "investing_cash_flow": -50_000_000,
            "financing_cash_flow": -40_000_000}
    within = {"cash_flow": {**base, "net_cash_flow": 10_000_900}}
    assert financial_guards.check(within) == []
    beyond = {"cash_flow": {**base, "net_cash_flow": 10_002_000}}
    assert [i for i in financial_guards.check(beyond) if i["check"] == "cash_flow_sums"]


def test_duplicate_share_count_across_statements_is_allowed():
    metrics = {"balance_sheet": {"total_common_shares": 354_568_000},
               "income_statement": {"diluted_shares_outstanding": 354_568_000}}
    assert [i for i in financial_guards.check(metrics)
            if i["check"] == "duplicate_value_across_statements"] == []
    metrics = {"balance_sheet": {"cash": 5_000_000},
               "income_statement": {"total_revenue": 5_000_000}}
    assert [i for i in financial_guards.check(metrics)
            if i["check"] == "duplicate_value_across_statements"]


def test_annual_balance_maps_debt_cash_and_common_shares():
    out = annual.transform_metrics(
        {"totalDebt": 1_000, "cash": 200, "totalCommonShares": 500}, "bs"
    )
    assert out["total_debt"] == 1_000
    assert out["cash"] == 200
    assert out["total_common_shares"] == 500


def test_annual_depreciation_maps_to_depreciation():
    out = annual.transform_metrics({"depreciation": 500}, "income")
    assert out["depreciation"] == 500
    assert "amortization_of_intangible_assets" not in out


def test_annual_common_shares_are_not_fx_converted():
    rate = annual.get_sgd_rate("2023-12-31", "USD")
    out = annual.transform_metrics(
        {"totalDebt": 1_000, "cash": 200, "totalCommonShares": 500},
        "bs", "USD", "2023-12-31",
    )
    assert out["total_common_shares"] == 500
    assert out["total_debt"] == 1_000 * rate
    assert out["cash"] == 200 * rate
