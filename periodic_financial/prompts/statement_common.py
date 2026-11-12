from pydantic import BaseModel, Field
from typing import Literal


# The currencies in the MAS rate table (quarterly_rates.json) plus SGD, so the
# model must answer with a code the SGD conversion can use, never a symbol
CurrencyCode = Literal[
    "SGD",
    "USD",
    "EUR",
    "GBP",
    "AUD",
    "CAD",
    "CHF",
    "CNY",
    "HKD",
    "IDR",
    "INR",
    "JPY",
    "KRW",
    "MYR",
    "PHP",
    "THB",
    "TWD",
]


class ExtractedValue(BaseModel):
    value: float | None = Field(
        description="The number exactly as printed in the target column, without thousands separators and without applying the unit. Null when the line is not on the pages"
    )

    label: str | None = Field(
        description="The printed line label only, at most 15 words, e.g. 'Total assets'"
    )

    explanation: str | None = Field(
        description="One short sentence on how the value was chosen: which page, line and column, or why it is null"
    )


class StatementBase(BaseModel):
    currency: CurrencyCode | None = Field(
        description="ISO 4217 code of the currency printed in the unit row or column header, never the printed symbol: S$ is SGD, US$ is USD, RM is MYR, RMB is CNY, HK$ is HKD, P is PHP, Rp is IDR, Baht is THB. Null when no currency is printed or it is not one of the listed codes"
    )

    unit: Literal["units", "thousands", "millions", "billions", "unknown"] = Field(
        description="The scale printed on these pages, e.g. S$'000 is thousands, US$m is millions"
    )

    column: str | None = Field(
        description="The header of the column the values were read from, exactly as printed"
    )


# Shared by all three statement prompts, so every statement reads columns,
# signs and scale the same way
SHARED_RULES = """
    PICK THE RIGHT COLUMN
    - Read only the column for the target period named in the user message.
    - The pages often show the prior year, the full year or the company next to
      the target period. Confirm the column header before reading any value.
    - Use group / consolidated figures, never company-only or trust-only columns.
    - Stapled securities (e.g. a hospitality trust stapled to a business trust):
      use the Stapled Group column, not the H-REIT or HBT column. The stapled
      group can be named after the trust itself: Q5T prints "Far East H-REIT"
      and "Far East H-Trust", and "Far East H-Trust" is the stapled group.

    SCALE
    - Read the scale once, from the statement's own pages: its title, unit row
      or column header, e.g. "S$'000", "US$m", "(in thousands)", "RM'000",
      "in thousand Baht". Every value on the statement shares that one scale.
    - A scale word ("thousand", "'000", "million", "'m", "bn") wins over a bare
      currency: "in thousand Baht" is thousands even if a column says just "THB".
    - When only a currency is printed ("US$", "S$", "RM") and no scale word, the
      figures are in units. Never assume thousands.
    - If no scale and no currency is printed on these pages, set unit to
      "unknown". Never guess the scale.
    - Report each value exactly as the digits are printed. Never multiply or
      divide it yourself.
    - A SGX listing does not mean SGD. Read the currency from the pages.

    VALUES
    - A value printed in brackets, e.g. (1,234), is negative unless the field
      description says to report it as a positive number.
    - "-", a blank cell, or "*" with a footnote such as "amount less than
      $1,000" in the target column is null, not 0. A line that is not on the
      pages at all is also null. Only a printed 0 is 0.
    - Never compute a value that is not printed. Totals that are not printed
      are null.
    - label is the printed line label, so the value can be traced back to it.
    - explanation says in one short sentence where the value came from (page,
      line, column) or why it is null, e.g. "PDF_PAGE 4, 'Taxation', six-month
      2026 column prints * (less than $1,000)". Keep it under 25 words.

    BEFORE YOU RETURN
    - Compare a few large figures with the comparative column printed beside
      the one you read. They should be within a few times of each other, never
      about 1,000 times apart. A 1,000x gap means the wrong column or scale, so
      re-read it.
    - Run the checks listed for this statement below. When one fails, re-read
      the lines involved and correct them instead of returning a known mismatch.
"""


# Added to the user prompt when a first reading broke a statement identity
RECHECK_NOTE = """
    RECHECK
    An earlier reading of these pages broke a rule that every printed statement
    satisfies:

{breaks}

    At least one of these figures came from the wrong line or the wrong column.
    Read every field again from the target column only, and confirm the column
    header for each of these lines before reading its value.
"""
