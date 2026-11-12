from pydantic import BaseModel, Field
from typing import Literal

from .statement_common import CurrencyCode


# EBITDA is rarely on the income statement itself, companies print it in the
# financial highlights, the review of performance or the segment note, so it
# is read in its own step from the pages that print it
class EbitdaReading(BaseModel):
    value: float | None = Field(
        description="The group EBITDA exactly as printed for the target period, without thousands separators and without applying the unit. Negative when printed in brackets. Null when it is not printed for the target period"
    )

    label: str | None = Field(
        description="The printed label, at most 10 words, e.g. 'EBITDA' or 'Adjusted EBITDA'"
    )

    is_adjusted: bool = Field(
        description="True when the label qualifies EBITDA, e.g. adjusted, underlying, core, normalised, recurring, or 'excluding' something"
    )

    unit: Literal["units", "thousands", "millions", "billions", "unknown"] = Field(
        description="The scale of that figure, e.g. S$'000 is thousands, 'S$45.3 million' is millions"
    )

    currency: CurrencyCode | None = Field(
        description="ISO 4217 code of the figure's currency, never the symbol: S$ is SGD, US$ is USD, RM is MYR, RMB is CNY, HK$ is HKD. Null when none is printed"
    )

    coverage: Literal["six_months", "twelve_months", "three_months", "other"] | None = (
        Field(description="The period the figure covers")
    )

    page: int | None = Field(description="The n from [PDF_PAGE n] the figure is on")

    explanation: str | None = Field(
        description="One short sentence: which page, table or sentence and column, or why it is null"
    )


SYSTEM_PROMPT = """
    You read one figure from an SGX-listed company's results announcement: the
    group's EBITDA for the target period.

    WHAT COUNTS
    - Only a figure the company labels EBITDA: "EBITDA", "Adjusted EBITDA",
      "Underlying EBITDA", "EBITDA (excluding ...)", or the name written out,
      e.g. "Earnings before interest, taxes, depreciation and amortisation"
      (U9E). It can be a table row or a sentence, e.g. "EBITDA rose 12% to
      S$45.3 million". A label can wrap over several lines with the figure on
      the first, e.g. BTE's "Earnings before tax, non-controlling interests,
      interest on borrowings, depreciation and amortisation and foreign
      exchange (loss)/gain ("EBITDA")".
    - The group or consolidated total. Never a segment, business unit,
      geography, property or company-only figure.
    - In a segment note, a total counts only when it is the whole group: a
      column labelled "Group" or "Consolidated", or a "Total" that adds every
      segment including "Others" or corporate, with no unallocated or
      corporate expenses reconciled below it, e.g. QES's "Total" across Rubber
      Chemicals, Heating Power, Waste Treatment and Others. A total of the
      reportable segments that the note then reconciles with unallocated or
      corporate expenses is not group EBITDA, e.g. 42C prints "Adjusted
      EBITDA" for its two segments with a Total of (949), then "Unallocated
      corporate expenses (2,057)": return null.
    - Never read an unlabelled subtotal, even one that a reconciliation
      implies is EBITDA.
    - When both plain EBITDA and an adjusted or underlying EBITDA are printed,
      read the plain one, unless the plain one is a segment total that fails
      the segment rule below. Q01 prints "Segment EBITDA" in its Consolidated
      column, then "Unallocated expenses (7,807)" below it, so it is not group
      EBITDA: read the review's "Group EBITDA before exceptional items".
    - Never an EBITDA margin, a ratio, a per-share figure or a growth rate.
    - Never calculate EBITDA yourself, e.g. by adding depreciation to operating
      profit. If it is not printed, return null.

    PICK THE RIGHT PERIOD
    - Read only the figure for the target period in the user message.
    - The pages often show the prior period, the full year or a quarter next to
      it. Confirm the column header or the sentence's period before reading.
    - For a second-half target, a full-year report may print the six months
      and the twelve months. Read the six months. When only the twelve months
      is printed, return that twelve-month value with coverage set to
      twelve_months, never null: the code subtracts the first half.
    - A quarter figure is three_months. Never add quarters together.
    - Some tables put a % variance column between the period columns, and
      the header row does not show it: match each number to its column with
      the unit row under the headers. BVA's header reads "2QFY2025 2QFY2024
      1HFY2025 1HFY2024 Variance Variance", its unit row "RM'mil RM'mil %
      RM'mil RM'mil %", and its EBITDA row "132 29 355 224 50 348": 1HFY2025
      is 224, and 355 is a percentage. Never read a value from a % column.

    SCALE AND SIGN
    - Read the scale printed with that figure: its table header, unit row or
      the words in the sentence ("million", "'000", "m", "bn"). Highlights
      often use a different scale from the statements.
    - A figure in brackets is negative.

    Return the response in the following JSON schema:
    {format_instructions}
"""

USER_PROMPT = """
    Target period: {target_period}

    Pages that mention EBITDA:

    {pages}
"""
