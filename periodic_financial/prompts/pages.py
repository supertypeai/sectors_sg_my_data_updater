from pydantic import BaseModel, Field
from typing import Literal


class TargetPages(BaseModel):
    page: int = Field(
        description="The number inside [PDF_PAGE n of N], never the page number printed in the document"
    )

    evidence: str = Field(
        description="The statement title exactly as written on that page"
    )

    period_covered: str = Field(
        description="The period line under the statement title, e.g. 'Six months ended 31 December 2024', 'Year ended 31 December 2024' or 'As at 31 December 2024'"
    )

    coverage: Literal["period", "full_year"] = Field(
        description="'period' when the page presents the target period, alone or as a column next to the full year; 'full_year' when it only presents the twelve months"
    )


class StatementPages(BaseModel):
    has_financial_statements: bool = Field(
        description="False when the document has no primary financial statements, e.g. a slide deck, press release or results summary"
    )

    income_statement: list[TargetPages] = Field(
        description="Every page of the consolidated income statement, in page order"
    )

    balance_sheet: list[TargetPages] = Field(
        description="Every page of the consolidated balance sheet, in page order"
    )

    cash_flow: list[TargetPages] = Field(
        description="Every page of the consolidated cash flow statement, in page order"
    )

    notes: str | None = Field(
        default=None,
        description="Anything ambiguous about the choice, e.g. two candidate pages for one statement",
    )


SYSTEM_PROMPT = """
    You locate the primary financial statements inside an SGX-listed company's
    periodic financial report (half-year, full-year or quarterly).

    You receive the first lines of every page. Each page starts with a label
    [PDF_PAGE n of N]. That label is the page's position in the file.
    You only see the start of each page, so the figures below the headers are
    not shown. Judge each page from its title, period line and column headers.

    PAGE NUMBERS
    - Always answer with the n from [PDF_PAGE n of N].
    - Never use a page number printed in the document ("Page 3", "- 1 -") or listed
    in a contents/index page. Printed numbers often differ from the PDF position
    because cover and index pages are not numbered.

    WHAT TO FIND
    Return the pages of the group / consolidated statements only:

    1. income_statement: the statement that goes from revenue down to profit for
    the period, with the "Attributable to:" lines and earnings per share.
    Common titles:
    - Income Statement, Consolidated Income Statement
    - Statement of Profit or Loss
    - Statement of Profit or Loss and Other Comprehensive Income (one combined
        statement, counts as the income statement)
    - Statement of Comprehensive Income, when it is the only statement that
        presents revenue and profit
    - REITs and business trusts: Statement of Total Return, Statement of Total
        Return and Distribution, Income and Distribution Statement, Profit and
        Loss and Distribution Statement

    2. balance_sheet: Statement of Financial Position, Balance Sheet.

    3. cash_flow: Statement of Cash Flows, Cash Flow Statement.

    A statement that runs over several pages: include every page. A continuation
    page usually repeats the title or adds "(continued)" / "(cont'd)".

    A page marked "(page content is an image)" still counts. Choose it from its title.

    TARGET PERIOD
    The user message names the period being extracted (H1 or H2) and its end date.
    - Read the period from the line under each statement's title, not from the
      running header at the top of the page. A full-year report can print
      "Six-month period ended ..." in its header on every page, including pages
      whose statement says "Year ended ...".
    - A page with the target period and other periods side by side as columns
      (e.g. "6 months ended" next to "12 months ended") is fine. Return it.
    - When the report presents the same statement twice, once for the target
      period and once for another period (e.g. a six-month income statement on
      some pages and a full-year one on others), return only the target period's
      pages.
    - When a statement exists only for another period (e.g. a full-year report
      whose cash flow statement covers the year only), still return its pages,
      write the period it covers in period_covered and say so in notes.
    - The balance sheet is "as at" the target period's end date.

    COVERAGE
    Set coverage on every page you return:
    - "period": the page presents the target period, either on its own or as a
      column next to the full year (e.g. "6 months ended" beside "12 months
      ended", "2H 2024" beside "FY 2024"). A balance sheet as at the target end
      date is always "period".
    - "full_year": the page only presents the twelve months (e.g. "Year ended
      31 December 2024" with no six-month column), so the target period's
      figures are not printed on it. A quarter printed beside the full year
      is not the target period either: ME8U prints "4Q FY23/24 (three months
      ended 31 March 2024)" beside "FY23/24 (year ended 31 March 2024)" with
      no six-month column, which is "full_year".

    WHAT TO SKIP
    - A separate Statement of Comprehensive Income that starts from "Profit for the
    period" and only adds other comprehensive income, when an income statement
    page exists elsewhere. It has its own "Attributable to:" block for total
    comprehensive income, which is not the profit figure.
    - Company-only or trust-only statements when consolidated ones exist. If one
    page shows Group and Company columns side by side, that page is fine.
    - Notes to the financial statements, "Explanatory notes to ...", segment
    information, reviews of performance and commentary.
    - Summary, highlights or "Summary of results" pages.
    - Statement of Changes in Equity, Statement of Movements in Unitholders' Funds,
    Distribution Statement on its own page.
    - Cover, contents and index pages.

    If the document has no primary statements, for example a slide deck or a
    supplementary information pack, set has_financial_statements to false and
    return empty lists. Do not guess pages.

    Return the response in the following JSON schema:
    {format_instructions}
"""

TARGET_PERIODS = {
    "H1": "H1, the first half of the financial year: six months ended {date}. The report is a half-year results announcement.",
    "H2": "H2, the second half of the financial year: six months ended {date}. The report is a full-year results announcement, so it usually also presents the twelve months ended {date}.",
}

# A full-year report that prints a statement for the twelve months only. The
# code subtracts the stored H1 row from these figures to get H2
FULL_YEAR_TARGET_PERIOD = "FY, the full financial year: twelve months ended {date}. This statement only presents the twelve months, so read the twelve-month column."

# A half the company never filed on SGX is read from the comparative column of
# its next report (ULG's 1H2025 from the 1H2026 report)
COMPARATIVE_NOTE = " This report is for a later period ending {document_date}. Read only its comparative column for the period ended {date}, never the current column."

USER_PROMPT = """
    Find the consolidated income statement, balance sheet and cash flow statement
    pages in this report.

    Target period: {target_period}

    The report has {page_count} pages. First lines of each page:

    {page_headings}
"""
