from periodic_financial.prompts.statement_common import CurrencyCode

from pydantic import BaseModel, Field
from typing import Literal


class ForecastYear(BaseModel):
    label: str = Field(
        description="The column heading exactly as printed, e.g. 'FY26F', '2027F*', 'FY2026F'"
    )

    period_end: str = Field(
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="Year and month the financial year ends, YYYY-MM, e.g. '2027-03' for the year to 31 March 2027",
    )

    revenue: float | None = Field(
        default=None,
        description="Revenue as printed, in forecast_basis.amounts_currency and amounts_unit. Banks: total income. REITs: gross revenue",
    )

    net_profit: float | None = Field(
        default=None,
        description="The profit line the house forecasts (net profit, PATMI, core net profit, distributable income for a REIT), in forecast_basis.amounts_currency and amounts_unit",
    )

    eps: float | None = Field(
        default=None,
        description="Earnings per share as printed, in forecast_basis.eps_currency and eps_unit",
    )

    dps: float | None = Field(
        default=None,
        description="Dividend per share, or distribution per unit for a REIT, as printed, in forecast_basis.dps_currency and dps_unit",
    )

    dividend_yield: float | None = Field(
        default=None,
        description="Dividend or distribution yield as a fraction: 4.6% is 0.046",
    )

    pe: float | None = Field(default=None, description="P/E in times")

    pb: float | None = Field(default=None, description="P/B or P/BV in times")


CURRENCY_HINT = "ISO 4217 code, never the printed symbol: S$ is SGD, US$ is USD, RM is MYR, RMB is CNY, HK$ is HKD, Rp is IDR, Baht is THB, pence is GBP"

PER_SHARE_HINT = "'major' for whole units (S$0.12, US$0.05), 'minor' for cents, pence, sen or fen (12 cents)"


class ForecastBasis(BaseModel):
    amounts_currency: CurrencyCode | None = Field(
        default=None,
        description=f"Currency of revenue and net profit. {CURRENCY_HINT}",
    )

    amounts_unit: Literal["units", "thousands", "millions", "billions"] | None = (
        Field(
            default=None,
            description="Scale of revenue and net profit as printed: S$m and SGD mn are millions, '000 is thousands",
        )
    )

    eps_currency: CurrencyCode | None = Field(
        default=None, description=f"Currency of EPS. {CURRENCY_HINT}"
    )

    eps_unit: Literal["major", "minor"] | None = Field(
        default=None, description=PER_SHARE_HINT
    )

    dps_currency: CurrencyCode | None = Field(
        default=None,
        description=f"Currency of DPS, which can differ from the reporting currency. {CURRENCY_HINT}",
    )

    dps_unit: Literal["major", "minor"] | None = Field(
        default=None, description=PER_SHARE_HINT
    )

    profit_type: Literal["reported", "core", "distributable"] | None = Field(
        default=None,
        description="Kind of profit line in net_profit. 'reported' for net profit, PATMI, profit attributable to owners or total return. 'core' when the house strips one-off items: adjusted, core, normalised, underlying or pre-exceptional profit. 'distributable' for a REIT's distributable or distribution income",
    )


class AnalystView(BaseModel):
    covers_stock: bool = Field(
        description="True when the pages give this stock its own rating or target price"
    )

    analyst: str | None = Field(
        default=None,
        description="The named analyst for this stock, the first one when several are listed",
    )

    rating: str | None = Field(
        default=None,
        description="The rating word first, then any qualifier in brackets, e.g. 'BUY (Maintained)', 'ADD (no change)', 'ACCUMULATE'",
    )

    target_price: float | None = Field(
        default=None, description="The 12-month target price for this stock"
    )

    target_currency: CurrencyCode | None = Field(
        default=None, description="Currency of target_price"
    )

    previous_target_price: float | None = Field(
        default=None,
        description="The target before this report, only when the report prints it",
    )

    target_basis: str | None = Field(
        default=None,
        description="How the target is derived, in the report's words, e.g. '3.33x 2027F P/B from a Gordon growth model'",
    )

    forecast_basis: ForecastBasis | None = Field(
        default=None, description="Units of the forecast table, null when there is none"
    )

    forecasts: list[ForecastYear] = Field(
        default_factory=list,
        description="Forecast years only (E or F), oldest first. Leave out actual (A) years",
    )

    notes: str | None = Field(
        default=None,
        description="Anything ambiguous, e.g. two targets for different share classes",
    )


SYSTEM_PROMPT = """
    You read a broker research report on stocks listed on the Singapore
    Exchange and return one stock's rating, target price and forecasts.

    You receive selected pages of the report. Each page starts with a label
    [PDF_PAGE n]. Some reports cover one company, others cover a sector or the
    whole market and give many stocks a row each. Read only the stock named in
    the user message. When a table lists several stocks, take the figures from
    this stock's row.

    RATING AND TARGET
    - Write the rating word first, then any qualifier in brackets: "BUY
      (Maintained)", "ADD (previously HOLD)". "Maintain BUY" becomes
      "BUY (Maintained)".
    - target_price is this stock's target or fair value, as a number.
    - previous_target_price only when the report prints the old target, e.g.
      "raise TP to S$84.70 from S$83.60" or a "Previous TP" field. Never infer it.
    - When the pages give this stock no rating and no target, set covers_stock
      to false and leave the other fields empty.

    FORECASTS
    - Take the house's own forecast table for this stock: the key financials
      box, the financial summary, or the stock's row in a coverage table.
    - One entry per forecast year (E or F columns). Skip actual (A) years.

    FINANCIAL YEAR
    - Copy each column heading into label and give the month its year ends
      in period_end. Most companies end in December, so FY26F is 2026-12.
    - For other year ends, houses label the same year differently. For a
      company whose year ends 31 March, the year April 2026 to March 2027 is
      "2027F" or "FY27" at some houses and "FY26E" or "FY2026F" at others.
    - Settle it from the page: a "Year to 31 Mar" or "FYE Mar" note, a
      "FY26/27" form, the half year just reported ("1HFY27" for the six
      months to September 2026), and the report date. The first forecast
      year is usually the one in progress when the report is published.
    - Check against the figures. A year whose revenue is a small fraction of
      the next is a short period, such as the weeks between a listing and the
      first year end, and it ends before the full years that follow.
    - Copy each number as printed, without thousands separators. Brackets mean
      negative. Do not convert units or currencies, name them in forecast_basis
      as a currency code and a scale.
    - Take the currency and scale from the table's own header or unit row:
      "FY Dec (IDRmn)" is IDR millions, "IDRbn" is IDR billions, "(S$ '000)" is
      SGD thousands. A company that reports in IDR, USD or another currency is
      not SGD just because the stock trades in Singapore.
    - Check the scale against amounts the text states, and when the two
      disagree the text wins, since houses mislabel table headers. When the
      text gives half-year revenue of S$117.3 million and the table prints
      160744 for the full year, the table is in thousands even if its header
      says millions. When the text says earnings reach IDR 2.6tr and the table
      prints net profit of 2,631 under "(IDRmn)", the table is in billions.
    - Yields are fractions: 4.6% is 0.046.
    - Leave a field null when the report does not print it. Do not compute
      EPS, ratios or yields yourself.
    - Consensus, Bloomberg, management guidance and prospectus forecasts are
      not the house's forecasts. Skip them.

    Return the response in the following JSON schema:
    {format_instructions}
"""

USER_PROMPT = """
    Stock: {company_name} (SGX code {security_code})
    Also written as: {aliases}
    Report: {title}
    Research house: {contributor}
    Published: {report_date}

    Pages:

    {pages}
"""
