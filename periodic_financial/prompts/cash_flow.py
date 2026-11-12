from pydantic import Field

from .statement_common import ExtractedValue, StatementBase, SHARED_RULES


class CashFlow(StatementBase):
    operating_cash_flow: ExtractedValue = Field(
        description="Net cash from/(used in) operating activities. An outflow is negative"
    )

    investing_cash_flow: ExtractedValue = Field(
        description="Net cash from/(used in) investing activities. An outflow is negative"
    )

    financing_cash_flow: ExtractedValue = Field(
        description="Net cash from/(used in) financing activities. An outflow is negative"
    )

    capital_expenditure: ExtractedValue = Field(
        description="Purchase of property, plant and equipment. Printed in brackets is spending, report it as a positive number. Printed without brackets is a net inflow, report it as a negative number"
    )

    intangible_asset_purchases: ExtractedValue = Field(
        description="Purchase of intangible assets, e.g. software, only when printed as its own line. Printed in brackets is spending, report it as a positive number. Printed without brackets is a net inflow, report it as a negative number"
    )

    other_capital_purchases: list[ExtractedValue] = Field(
        description="Each other capital expenditure line, one item per line: spending on investment properties the group owns or is building (capital expenditure on, additions to, improvement or subsequent expenditure on investment properties), construction in progress, exploration and evaluation assets, mine properties, biological assets, land rights and land costs (land use rights, leasehold land, land premium, land under development), and any intangible assets line after the first. A line printed in brackets is spending, report it as a positive number. A line printed without brackets is a net inflow, report it as a negative number. Empty when there are none"
    )

    effect_of_exchange_rate_changes: ExtractedValue = Field(
        description="Effect of exchange rate changes on cash and cash equivalents. Negative when printed in brackets"
    )

    free_cash_flow: ExtractedValue = Field(
        description="Free cash flow, only when a line is labelled free cash flow"
    )

    net_cash_flow: ExtractedValue = Field(
        description="The printed net increase/(decrease) in cash and cash equivalents line"
    )


SYSTEM_PROMPT = (
    """
    You extract figures from the consolidated cash flow statement of an
    SGX-listed company's periodic financial report. You receive only the cash
    flow statement pages, each starting with a [PDF_PAGE n] label.
"""
    + SHARED_RULES
    + """
    CASH FLOW RULES
    - Read only the target period's column. A full-year report may show six
      months and twelve months side by side; never read the twelve-month column
      for a six-month target.
    - capital_expenditure is the single printed line for purchases of property,
      plant and equipment. Purchases of intangible assets go in
      intangible_asset_purchases, never into capital_expenditure. A line
      that combines intangibles with business acquisitions, e.g. K6S's
      "Acquisition of business and intangibles", is intangible_asset_purchases
      in every period. Do not add
      several lines together, the code adds them. Keep the printed
      direction: spending is printed in brackets, report it as a positive
      number. A line printed without brackets is a net inflow, report it as
      a negative number, e.g. BTM's 2H2023 "Additions to property, plant and
      equipment 4,523" is -4,523. The code leaves inflows out.
    - other_capital_purchases lists every other capital expenditure line, one
      item per line:
      - spending on investment properties the group already owns or is
        building, whatever the label: "Capital expenditure on investment
        properties", "Additions to investment properties (under
        construction)", "Improvement to investment property", "Subsequent
        expenditure on investment properties", "Payment for additions to an
        investment property", and a plain "Payments for investment
        properties" (U14). List these in every period they are printed,
        also when the line sits apart from the other investing lines, e.g.
        5VS prints "Addition to investment properties (120)" on its own.
      - a line that covers both buying and developing investment properties,
        e.g. 9CI's "Acquisition of/development expenditure in investment
        properties": list it, it cannot be split.
      - Keep the printed direction: spending is printed in brackets, report it
        as a positive number. A line printed without brackets is a net
        inflow, report it as a negative number, e.g. 9CI's "Net
        disposal/(acquisition of/development expenditure in) of investment
        properties 52" is -52. The code leaves inflows out.
      - a second intangible assets line, when intangible_asset_purchases
        already holds one, e.g. B58's "Additions to intangible assets (236)"
        goes in intangible_asset_purchases and "Subsequent expenditure on
        trademarks (579)" goes here.
      - spending on assets being built: "Increase in construction in
        progress", "Additions to capital work in progress", "Payments for
        assets under construction".
      - exploration and evaluation assets, mine properties, biological assets
        (e.g. livestock, plantations).
      - land rights and land costs, whatever the label: "Additions to land
        use rights" (EB5, P8Z), "Acquisition of land use rights" (I07),
        "Additions to leasehold land under right-of-use assets" (J36),
        "Payment of (upfront) land premium" (M1GU, 8C8U, OU8), "Payment for
        leasehold land development cost" (Z74), "Expenditure on land under
        development" (S20), "Installments paid for purchase of land".
    - Buying a property is not capital expenditure: "Purchase of investment
      properties", "Acquisition of (interests in) investment properties",
      "Payment for purchase of investment properties". Neither are
      acquisitions of subsidiaries or investments, or right-of-use asset
      additions other than land. List them nowhere.
    - net_cash_flow is the printed net increase/(decrease) line, before cash at
      the beginning of the period. When one is printed before and one after
      the currency translation effect (NC2: "Net increase ... before effect
      from currency translation 39,454,343", then "Effect from currency
      translation 23,070,402", then "Net increase 62,524,745"), use the one
      before the effect.
    - effect_of_exchange_rate_changes is the "Effect of exchange rate changes"
      line, wherever it is printed. It can also be labelled e.g. "Translation
      adjustments" or "Effect of foreign exchange". Null when there is no such
      line.

    CHECKS
    - operating_cash_flow + investing_cash_flow + financing_cash_flow =
      net_cash_flow, give or take rounding, plus effect_of_exchange_rate_changes
      when that line is printed above the net line.

    Return the response in the following JSON schema:
    {format_instructions}
"""
)

USER_PROMPT = """
    Target period: {target_period}
    Period line on these pages: {period_covered}

    Cash flow statement pages:

    {statement_pages}
"""
