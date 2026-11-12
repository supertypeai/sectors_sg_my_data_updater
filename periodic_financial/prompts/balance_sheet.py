from pydantic import Field

from .statement_common import ExtractedValue, StatementBase, SHARED_RULES


class BalanceSheet(StatementBase):
    total_asset: ExtractedValue = Field(
        description="Total assets, only when a 'Total assets' line is printed"
    )

    total_current_asset: ExtractedValue = Field(description="Total current assets")

    total_non_current_asset: ExtractedValue = Field(
        description="Total non-current assets"
    )

    total_liabilities: ExtractedValue = Field(
        description="Total liabilities, only when a 'Total liabilities' line is printed. Report as a positive number"
    )

    total_current_liabilities: ExtractedValue = Field(
        description="Total current liabilities. Report as a positive number"
    )

    total_non_current_liabilities: ExtractedValue = Field(
        description="Total non-current liabilities. Report as a positive number"
    )

    total_equity: ExtractedValue = Field(
        description="Total equity including non-controlling interests and perpetual securities, or net assets for REITs and trusts"
    )

    # Only for the funds employed layout, code derives the totals from it
    capital_employed: ExtractedValue = Field(
        description="Only in a 'funds employed' layout that lists equity and non-current liabilities first: the bare total line closing them, equal to non-current assets plus net current assets. Null in any other layout"
    )

    working_capital: ExtractedValue = Field(
        description="Working capital, only when printed as one line labelled 'Working capital' or 'Net current assets' ('Net current liabilities' is negative)"
    )


SYSTEM_PROMPT = (
    """
    You extract figures from the consolidated balance sheet (statement of
    financial position) of an SGX-listed company's periodic financial report.
    You receive only the balance sheet pages, each starting with a [PDF_PAGE n]
    label.
"""
    + SHARED_RULES
    + """
    BALANCE SHEET RULES
    - Read the column "as at" the target period's end date, not the comparative
      date (usually the previous financial year end).
    - total_equity is the whole equity of the group: equity attributable to
      owners or unitholders, plus perpetual securities, plus non-controlling
      interests. For REITs and trusts this is usually "Net assets" or the total
      under "Represented by:".
    - Never report "Unitholders' funds" or "Equity attributable to owners" alone
      as total_equity when non-controlling interests or perpetual securities
      are shown next to it.
    - Assets and liabilities are amounts, always positive. Some groups print
      every liability in brackets and end on "Net assets" (D01 "Current
      liabilities (3,397.8)", HMN "Total Liabilities (3,731,291)"); the
      brackets only mark a deduction there, so report the liabilities as
      positive numbers. Only working_capital and total_equity can be negative.
    - Only read total_asset and total_liabilities from lines labelled as such
      ("Total assets", "Total liabilities"). Otherwise leave them null; they
      are derived later from the other fields.

    SECTION SUBTOTALS
    - Many balance sheets print the current / non-current subtotals without a
      "Total ..." label: a bare number line directly after the last item of a
      section headed "Current assets", "Non-current assets", "Current
      liabilities" or "Non-current liabilities". Read that number as the
      section's total, and set label to e.g. "Subtotal of Current assets".
    - Only use a bare line that closes exactly one section. A bare line after
      two sections, or after equity, is not a section total.
    - "Funds employed" layouts put equity and non-current liabilities above the
      assets. There the bare line after equity and long-term liabilities is
      equity + non-current liabilities, not total assets. Report it as
      capital_employed, e.g. C6L prints equity, then "Deferred account",
      "Borrowings" and other long-term lines, then a bare "28,366.7", then
      "Represented by:" the assets. That layout often prints no total assets
      or total liabilities line: leave them null, code derives them.
    - When "Assets held for sale" sits on its own line after the current assets
      subtotal, total_current_asset is the line that includes it: a "Total
      current assets" line, or a bare number line directly after the held for
      sale line that closes the section. If neither is printed,
      total_current_asset is the "Current assets" subtotal plus the held for
      sale line, also when the subtotal is labelled "Current assets" and the
      held for sale amount is small, e.g. U96 prints "Current assets 3,072",
      then "Assets held for sale 17": report 3,072 + 17 and say so in
      explanation. "Liabilities held for sale" or "Liabilities directly
      associated with assets held for sale" after the current liabilities
      subtotal is added to total_current_liabilities the same way.

    CHECKS
    - total_asset = total_liabilities + total_equity.
    - total_asset = total_current_asset + total_non_current_asset.
    - total_liabilities = total_current_liabilities + total_non_current_liabilities.

    Return the response in the following JSON schema:
    {format_instructions}
"""
)

USER_PROMPT = """
    Target period: {target_period}
    Period line on these pages: {period_covered}

    Balance sheet pages:

    {statement_pages}
"""
