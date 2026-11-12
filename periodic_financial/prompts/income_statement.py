from pydantic import Field
from typing import Literal

from .statement_common import ExtractedValue, StatementBase, SHARED_RULES


class IncomeStatement(StatementBase):
    # Share counts carry their own scale ("Weighted average units ('000)")
    # that often differs from the money scale, the source of past 1,000x errors
    share_unit: Literal["units", "thousands", "millions", "unknown"] = Field(
        description="The scale of the weighted average share or unit count lines, e.g. '000 units is thousands. units when the full count is printed"
    )

    # Code calculates operating income and EBIT for other companies, a bank's
    # interest expense and allowances make those formulas wrong for it
    is_bank: bool = Field(
        description="True when the statement starts from interest income, as a bank or finance company's does"
    )

    total_revenue: ExtractedValue = Field(
        description="Total revenue / turnover / gross revenue. Exclude other income presented outside revenue"
    )

    # A list because some statements deduct several lines from revenue (AIY's
    # commission and interest expenses), one value made the model pick one
    # line in H1 and the subtotal after both in the full year
    cost_of_revenue: list[ExtractedValue] = Field(
        description="The line labelled cost of sales, cost of revenue, cost of services, direct costs or property operating expenses, or every line deducted from revenue before the gross profit or net revenue subtotal, one item per line. Report each as a positive number. Empty when there are none"
    )

    gross_income: ExtractedValue = Field(
        description="A labelled gross profit line, net property income for REITs, or a labelled net revenue line after revenue deductions. Null when the subtotal has no label"
    )

    operating_expense: list[ExtractedValue] = Field(
        description="The printed total of operating expenses excluding cost of revenue and finance costs as one item plus any operating expense line printed after it, or when no such total is printed, each operating expense line as its own item. Report an expense as a positive number and a line that reduces expenses as a negative number. Empty when there are none"
    )

    operating_income: ExtractedValue = Field(
        description="A labelled operating profit / profit from operations line, before finance costs and tax. Never an unlabelled subtotal"
    )

    non_operating_income_or_loss: list[ExtractedValue] = Field(
        description="Always empty, calculated in code"
    )

    interest_expense_non_operating: ExtractedValue = Field(
        description="Finance costs / interest expense. Report as a positive number"
    )

    pretax_income: ExtractedValue = Field(
        description="Profit before tax / total return before tax"
    )

    income_taxes: ExtractedValue = Field(
        description="Income tax for the period. Report an expense as a positive number and a tax credit as a negative number"
    )

    net_income: ExtractedValue = Field(
        description="Profit for the period / total return for the period, the total before it is split among owners, minorities and perpetual holders. Includes discontinued operations"
    )

    ebit: ExtractedValue = Field(
        description="A line labelled EBIT or 'Profit before interest and tax' (PBIT)"
    )

    ebitda: ExtractedValue = Field(
        description="EBITDA, only when a line is labelled EBITDA"
    )

    funds_from_operation: ExtractedValue = Field(
        description="Funds from operations (FFO), only when printed"
    )

    net_property_sales: ExtractedValue = Field(
        description="Gain/(loss) on divestment or sale of investment properties, only when printed"
    )

    minorities: ExtractedValue = Field(
        description="Profit attributable to non-controlling interests, from the 'Attributable to:' lines under profit for the period"
    )

    perpetual_security_holders: ExtractedValue = Field(
        description="Profit attributable to perpetual securities holders, from the 'Attributable to:' lines under profit for the period"
    )

    unitholders: ExtractedValue = Field(
        description="Profit attributable to unitholders, or to owners / equity holders of the company, from the 'Attributable to:' lines under profit for the period"
    )

    basic_shares_outstanding: ExtractedValue = Field(
        description="Weighted average number of shares or units for basic EPS/EPU, for the target period, from the statement or the earnings per share note pages"
    )

    diluted_shares_outstanding: ExtractedValue = Field(
        description="Weighted average number of shares or units for diluted EPS/EPU, for the target period, from the statement or the earnings per share note pages"
    )


SYSTEM_PROMPT = (
    """
    You extract figures from the consolidated income statement of an SGX-listed
    company's periodic financial report. You receive only the income statement
    pages, each starting with a [PDF_PAGE n] label.
"""
    + SHARED_RULES
    + """
    INCOME STATEMENT RULES
    - net_income is the total profit for the period before the "Attributable to:"
      split. When discontinued operations are shown, it includes them.
    - unitholders, minorities and perpetual_security_holders come from the
      "Attributable to:" lines directly under profit for the period. They add
      up to net_income.
    - Some statements print no "Attributable to:" lines. The earnings per
      share or unit note then shows the profit used, e.g. "Total return
      attributable to Unitholders 120,339", "Less: Distribution attributable to
      perpetual securities holders (4,013)" or "Profit attributable to equity
      holders of the Company". Only in that case, read unitholders (and
      perpetual_security_holders) from the note, for the target period's
      column, and say so in explanation.
    - Never read them from an "Attributable to:" block under total
      comprehensive income, and never from "Represented by:" or "Unitholders'
      funds" on a balance sheet. Those are not profit figures.
    - perpetual_security_holders is positive, even when the note prints it in
      brackets as a deduction.
    - Report income_taxes as a positive number for an expense, even when printed
      in brackets.
    - Some statements list expenses by nature (e.g. "Purchase of goods and
      consumables", "Employee benefits", "Depreciation") instead of a cost of
      sales line. Then cost_of_revenue is empty and gross_income is null.
      Never use one of those expense lines as cost_of_revenue, including lines
      named after a business ("Operating costs of maritime fund assets"): only
      a line labelled cost of sales, cost of revenue, cost of services, direct
      costs or property operating expenses counts, or a line deducted from
      revenue to reach a net revenue subtotal (next rule).
    - A REIT that prints "Other property income" between revenue and the
      property expenses counts it in revenue, since net property income
      includes it. MXNU prints "Revenue 18,375", "Other property income
      1,108", "Property operating expenses (1,358)", "Net property income
      18,125": total_revenue is 19,483 (18,375 + 1,108), say so in
      explanation.
    - Some statements deduct direct costs from revenue to reach a net revenue
      subtotal, e.g. "Revenue", "Less: Transaction-based expenses", "Net
      revenue". The subtotal is printed right after the deducted lines, before
      other income and the operating expenses. Every line deducted between
      total revenue and that subtotal is a cost_of_revenue item, e.g.
      "Commission and fee expenses" and "Interest expenses" both, and a
      labelled subtotal is gross_income.
    - A subtotal with no label is never gross_income, even when it sits where
      gross profit would. Leave gross_income null, the code calculates it as
      total_revenue minus the cost_of_revenue items.
    - Leave non_operating_income_or_loss empty. The code calculates it from
      pretax income, operating income and finance costs.
    - operating_income must be a labelled line. A bare subtotal with no label
      is never operating_income.

    BANKS AND FINANCE COMPANIES
    - When the statement starts from interest income (a bank or finance
      company), total_revenue is the total income line before operating
      expenses, e.g. "Total income", "Total operating income" or "Income before
      operating expenses": net interest income plus fee and other income.
      Never use gross interest income as total_revenue.
    - Their interest expense is part of that income, so cost_of_revenue is
      empty, and gross_income and interest_expense_non_operating are null.
    - Their operating_income is profit after allowances, before associates
      and tax. Read it from one of two places:
      1. A line labelled "Operating profit after allowances" or "Profit after
         allowances".
      2. When no such line is printed, and the allowances line is followed
         directly by profit before tax with no line between, that profit
         before tax line, because it is the profit after allowances (S41
         "Profit from operations before allowances", "Reversal or recovery of
         allowances", "Profit before tax").
      Never a "before allowances" line. Null when neither is printed.

    INSURERS
    - When the statement starts from "Insurance revenue" (G07), total_revenue
      is insurance revenue. cost_of_revenue is "Insurance service expenses"
      plus the "Net income/(expenses) from reinsurance contracts held" line
      (a net income there is a negative item), so gross profit is the printed
      "Insurance service result".
    - operating_expense is the "Other expenses" line printed lower down,
      under other income and expenses. Never list finance expenses from
      insurance contracts or finance costs.

    OPERATING PROFIT AND EBIT
    - operating_income is the company's operating profit line, e.g. "Operating
      profit", "Profit from operations", "Trading profit".
    - ebit is a line labelled EBIT or "Profit before interest and tax". It is
      often a different figure from operating_income (it can include associates
      and exceptional items), so read both lines separately.

    SHARE COUNTS
    - basic_shares_outstanding and diluted_shares_outstanding are the weighted
      average number of shares or units used for EPS/EPU. They are usually in
      the earnings per share note pages, which are given separately below the
      statement. Read the target period's column there too.
    - The note pages can contain other figures. Read only the share counts and,
      in the REIT case above, the attributable split from them. Never take
      revenue, profit or other statement lines from the notes.
    - Set share_unit from the scale printed next to the share count, e.g.
      "('000)" is thousands. It is often different from the money scale.
    - Only use a share count printed for the target period. A count printed
      only for the twelve months is not used for a six-month target.
    - When one share count is printed for both basic and diluted EPS (e.g.
      "for basic and diluted earnings per share"), or the EPS line reads
      "Basic and diluted", or the note says there is no dilution, use the
      basic count for diluted_shares_outstanding too.

    OPERATING EXPENSE
    - When a total of operating expenses that excludes cost of revenue and
      finance costs is printed, operating_expense is that line. Operating
      expense lines printed after that total and before finance costs are
      items too, e.g. B58 prints "Total costs and expenses", then an EBITDA
      line, then "Depreciation" and "Amortisation expense": list the total,
      the depreciation and the amortisation.
    - Otherwise list each operating expense line printed between gross profit
      (or revenue, when there is no cost of sales line) and finance costs, one
      item per line, e.g. "Selling and distribution expenses", "Administrative
      expenses", "Employee benefits expense", "Depreciation and amortisation",
      "Other operating expenses", or a REIT's "Manager's base fee", "Trustee's
      fee", "Audit fees". The code adds them up.
    - An expense line with indented sub-lines and its own subtotal is listed
      once: the subtotal or the sub-lines, never both. F83 prints
      "- Administrative", then "- Impairment loss on financial assets (529)"
      and "- Others (14,838)", then the subtotal "(15,367)": list 15,367 as
      Administrative, or the two sub-lines, not all three.
    - On a statement that lists expenses by nature, the material and purchase
      lines (e.g. "Material costs", "Purchase of goods and consumables") are
      operating expense lines too, since cost_of_revenue is empty there. So
      is "Changes in inventories" next to them.
    - A line among the expenses that is printed without brackets reduces
      them, e.g. 558 prints "Changes in inventories 42,786" between bracketed
      expense lines. List it as an item with a negative value, never leave it
      out. Its sign can differ between periods, read it for each column.
    - Never list cost_of_revenue, finance costs, share of results of associates
      or joint ventures, fair value changes, foreign exchange, impairment of
      investments or goodwill, tax, or any income line. These stay out even
      when printed among the expense lines, e.g. H30 prints "Exchange
      (loss)/gain, net" between its rental commission and other expenses.
    - Bank charges for borrowing are finance costs, e.g. "Commitment fee on
      unutilised bank facilities", "Loan arrangement fees". Never list them.
    - A "Total expenses" line that includes cost of sales or finance costs is
      not a total of operating expenses, list its operating lines instead.
    - Banks: only their printed total operating expenses line, e.g. "Total
      operating expenses" or "Total expenses", never the allowances.

    CHECKS
    - net_income = pretax_income - income_taxes, unless discontinued operations
      sit between them.
    - unitholders + minorities + perpetual_security_holders = net_income.
    - gross_income = total_revenue - the cost_of_revenue items.
    - A printed operating_income should equal gross_income (or total_revenue,
      when there is no cost of sales line) minus the operating_expense lines,
      give or take other income lines printed above it.

    Return the response in the following JSON schema:
    {format_instructions}
"""
)

USER_PROMPT = """
    Target period: {target_period}
    Period line on these pages: {period_covered}

    Income statement pages:

    {statement_pages}

    Earnings per share note pages (share counts, and the REIT attributable split
    only when the statement has none):

    {share_pages}
"""
