from pydantic import BaseModel, Field
from typing import Literal


StatementKind = Literal["income_statement", "balance_sheet", "cash_flow"]


class ScannedDocument(BaseModel):
    statements_found: list[StatementKind] = Field(
        description="Every primary consolidated statement presented as a full table of figures on the pages, empty when there is none"
    )

    statement_pages: list[int] = Field(
        description="The n from [PDF_PAGE n] of every page those statements are on"
    )

    is_period_shown: bool = Field(
        description="True when a statement page prints the target period end date, in a column header, the 'as at' line or the period line under the title"
    )

    document_type: Literal[
        "financial_statements", "presentation", "press_release", "summary", "other"
    ] = Field(description="What the document is, judged from all the pages")

    notes: str | None = Field(
        default=None,
        description="One short sentence on anything unclear, e.g. a statement printed for another period only",
    )


SYSTEM_PROMPT = """
    You check whether a scanned SGX filing contains the company's primary
    financial statements for one period. The pages are images, each labelled
    [PDF_PAGE n].

    PRIMARY STATEMENTS
    - income_statement: revenue down to profit for the period, e.g. Income
      Statement, Statement of Profit or Loss (and Other Comprehensive Income),
      and for REITs and trusts the Statement of Total Return.
    - balance_sheet: Statement of Financial Position, Balance Sheet.
    - cash_flow: Statement of Cash Flows, Cash Flow Statement.

    A statement counts only when it is the full table of line items with
    figures. These do not count:
    - Slides or charts showing a few headline figures.
    - Highlights, "Summary of results" or key figures tables.
    - Statement titles mentioned in the auditor's report or the notes.

    PERIOD
    The user message names the target period end date. Set is_period_shown to
    true only when that date is printed on a statement page, e.g. "As at 31
    December 2025", a column headed "31.12.2025", or "Six months ended 30 June
    2025". A date that only appears on the cover or in the notes does not count.

    Return the response in the following JSON schema:
    {format_instructions}
"""

USER_PROMPT = """
    Target period end date: {period_end}

    The document has {page_count} pages. The first {sent_page_count} are attached
    as images.
"""
