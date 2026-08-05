"""
Extracts the metrics in `data_example/metrics_scraped.json` out of an SGX
periodic-financial PDF (MD&A, results announcement, annual report, ...).

Models are called through OpenRouter's OpenAI-compatible API: DeepSeek
(`deepseek/deepseek-v4-flash`) for the normal text path, Grok (`x-ai/grok-4.3`)
as the fallback for PDFs with no text layer, because Grok accepts native `file`
input and the text-only models do not. Both are set per-run with --model /
--pdf-model.

Roughly $0.002 of OpenRouter credit per 55-page report on the default model: one
call, ~11k prompt tokens. Cost levers, in the order worth reaching for:
  --context-chars 16000            ~60% fewer prompt tokens (-> ~$0.001)
  --model deepseek/deepseek-v4-pro sharper, ~3x the cost
  --model moonshotai/kimi-k2.6     sharper still, ~9x
Prompt tokens dominate, so --context-chars moves the bill more than the model does.

The PDFs vary wildly — different issuers, layouts, section names and REIT/corporate
terminology — so nothing here pattern-matches a specific document. The pipeline is:

  1. Page text + tables are extracted locally (pdfplumber). Tables are appended as
     TSV because `extract_text()` mangles right-aligned numeric columns
     ("8 09" instead of "809"), and the TSV usually recovers them.
  2. A *retrieval* pass scores every page against a keyword profile per statement
     group, so only the handful of pages that actually contain the income
     statement / balance sheet / cash flow are sent to the model. A 55-page
     report costs ~3 short prompts instead of one enormous one.
  3. A period-detection call reads the cover/highlights pages and decides what
     "most recent period reported" means for THIS document — half year vs full
     year vs quarter — and what the corresponding table column is labelled. That
     label is handed to the extraction calls so they pick the right column out of
     tables that show 5-7 periods side by side.
  4. Extraction runs one structured-output call per statement group. Each field
     comes back with a value, its printed unit, the page it came from and a
     self-assessed confidence.
  5. Anything below MIN_CONFIDENCE (0.70) is written out as null.

Robustness notes:
  * OpenRouter routes one model across several providers and not all of them
    honour `response_format`, so every call walks a fallback ladder (strict schema
    -> schema without reasoning -> plain JSON mode with the schema inlined in the
    prompt) and the response is parsed leniently.
  * If the PDF has no usable text layer (a scan), the whole file is sent to the
    file-capable model with OpenRouter's `file-parser` plugin instead.

Usage:
    python financial_statement_rag.py data_example/"3. H1FY26-MDA.pdf" -o out.json

Requires OPENROUTER_API_KEY in the environment or in the repo's .env.
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pdfplumber
from dotenv import load_dotenv
from openai import OpenAI, APIStatusError, APITimeoutError, BadRequestError

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# Text path. 1M context, structured outputs, ~$0.002 per report. Step up to
# `deepseek/deepseek-v4-pro` (~3x) or `moonshotai/kimi-k2.6` (~9x) if accuracy
# on dense tables disappoints.
MODEL = "deepseek/deepseek-v4-flash"
# Scanned-PDF path — Kimi takes text/image only, Grok also takes `file`.
PDF_MODEL = "x-ai/grok-4.3"
# OpenRouter PDF plugin engine: mistral-ocr handles scans, native/cloudflare-ai
# are cheaper but weaker on image-only pages.
PDF_ENGINE = "mistral-ocr"

# What to extract from a full-year report. SGX issuers on half-yearly reporting
# present the second half alongside the full year ("Second Half Year", "2H 2025"),
# and the second half is the period that isn't already covered by the H1 filing.
# "full_year" restores the old behaviour of reading the 12-month column.
FULL_YEAR_BASIS = "second_half"

# Below this the value is discarded and reported as null, per spec.
MIN_CONFIDENCE = 0.70
# Per-group prompt budget. ~4 chars/token, so 24k chars is ~6k tokens of context.
# This is the main cost dial: prompt tokens dominate spend on a 55-page report.
MAX_CHARS_PER_GROUP = 24_000
# Budget for the merged single call, which sees the union of all three groups.
MAX_CHARS_TOTAL = 40_000
# A page with less text than this is treated as image-only (scan) rather than empty.
MIN_TEXT_CHARS_PER_PAGE = 120
# Pages returned per statement group before neighbour expansion.
TOP_K_PAGES = 4
# Tables are only worth their tokens on pages that are actually numeric.
MIN_NUMERIC_TOKENS_FOR_TABLES = 10
MAX_TABLE_CHARS = 4_000
# Output budget is sized from the schema instead of a flat ceiling — asking for
# 16k tokens gets a 402 on a small OpenRouter balance even though the reply needs
# well under 2k.
# 90 was too tight: each field returns value + unit + page + a source_label that
# can be a long printed line item, and replies were being cut off mid-JSON on the
# 33-field schema (~10% of a 281-report batch failed to parse).
TOKENS_PER_FIELD = 150
OUTPUT_TOKEN_OVERHEAD = 600
# Under this there isn't room for a usable answer, so fail loudly instead of
# returning a truncated one.
MIN_USEFUL_OUTPUT_TOKENS = 600
# Per-request ceiling. The SDK's default is 10 minutes and it retries twice on
# its own, so a stalled OpenRouter provider can hang for half an hour before
# raising anything; this fails fast enough to try another provider or model.
REQUEST_TIMEOUT = 120.0

METRICS_FILE = Path(__file__).parent / "data_example" / "metrics_scraped.json"

# Retrieval profiles. `phrases` are worth more than `terms` because a page that
# says "Statements Of Financial Position" is almost certainly the balance sheet,
# whereas "assets" alone appears everywhere.
GROUP_QUERIES = {
    "income_statement": {
        "phrases": [
            "income statement", "statement of profit or loss",
            "statement of comprehensive income", "operating revenue",
            "profit and loss", "results of operations", "statement of total return",
        ],
        "terms": [
            "revenue", "turnover", "ebitda", "ebit", "gross", "operating expenses",
            "profit", "tax", "attributable", "earnings", "eps", "shares",
            "interest expense", "finance costs", "distribution", "unitholders",
            "perpetual", "non-controlling",
        ],
    },
    "balance_sheet": {
        "phrases": [
            "statement of financial position", "balance sheet",
            "statements of financial position", "total assets", "total liabilities",
        ],
        "terms": [
            "assets", "liabilities", "equity", "current", "non-current",
            "borrowings", "payables", "receivables", "cash and cash equivalents",
            "net asset value", "shareholders",
        ],
    },
    "cash_flow": {
        "phrases": [
            "cash flow statement", "statement of cash flows", "cash flows from",
            "operating activities", "investing activities", "financing activities",
            "free cash flow",
        ],
        "terms": [
            "cash", "capex", "capital expenditure", "purchase of property",
            "proceeds", "dividends paid", "net increase", "net decrease",
        ],
    },
}

# Fed to the model so ambiguous / REIT-specific line items are read consistently
# instead of being guessed at per document.
FIELD_HINTS = {
    "total_revenue": "Total revenue / turnover / operating revenue for the period. Exclude 'other income' if it is presented outside revenue.",
    "cost_of_revenue": "Cost of sales / cost of services / direct costs.",
    "gross_income": "Gross profit = total_revenue - cost_of_revenue. Only report if presented or if both components are unambiguous.",
    "operating_expense": "Total operating expenses excluding cost of revenue where the two are presented separately; otherwise total operating expenses.",
    "operating_income": "Operating profit / profit from operations, before non-operating items and tax.",
    "non_operating_income_or_loss": "Net non-operating income/(loss): other income, FX, fair-value changes, share of associates, exceptional items.",
    "interest_expense_non_operating": "Finance costs / interest expense reported below operating profit. Report as a positive number.",
    "pretax_income": "Profit before tax / profit before income tax.",
    "income_taxes": "Income tax expense for the period. Report a tax expense as a positive number and a tax credit as negative.",
    "net_income": "Profit after tax for the period, before allocating to minorities/perpetual holders (i.e. total net profit).",
    "ebit": "Same figure as operating_income: report whichever of 'EBIT' or "
    "'operating profit / profit from operations' the issuer presents. If both are "
    "presented, use the operating profit figure.",
    "ebitda": "EBITDA, only if explicitly presented.",
    "funds_from_operation": "REIT metric: funds from operations (FFO). Null for non-REIT issuers unless explicitly presented.",
    "net_property_sales": "REIT/property metric: net gain or proceeds on sale of investment properties, only if explicitly presented.",
    "minorities": "Profit attributable to non-controlling / minority interests.",
    "perpetual_security_holders": "Profit or distribution attributable to perpetual securities holders.",
    "unitholders": "Profit / total return attributable to unitholders (REITs and trusts).",
    "basic_shares_outstanding": "Weighted average number of ordinary shares/units used for basic EPS/DPU.",
    "diluted_shares_outstanding": "Weighted average number of shares/units used for diluted EPS/DPU.",
    "total_asset": "Total assets as at the balance sheet date.",
    "total_current_asset": "Total current assets. Null if the issuer presents an unclassified balance sheet.",
    "total_non_current_asset": "Total non-current assets.",
    "total_liabilities": "Total liabilities.",
    "total_current_liabilities": "Total current liabilities.",
    "total_non_current_liabilities": "Total non-current liabilities.",
    "total_equity": "Total equity including non-controlling interests and perpetual securities.",
    "working_capital": "total_current_asset - total_current_liabilities. Compute only if both are reported.",
    "operating_cash_flow": "Net cash from/(used in) operating activities. Outflow is negative.",
    "investing_cash_flow": "Net cash from/(used in) investing activities. Outflow is negative.",
    "financing_cash_flow": "Net cash from/(used in) financing activities. Outflow is negative.",
    "capital_expenditure": "Payments for property, plant and equipment plus intangibles (cash capex). Report as a positive number.",
    "free_cash_flow": "Free cash flow, only if explicitly presented (do not derive it).",
    "net_cash_flow": "Net increase/(decrease) in cash and cash equivalents for the period.",
}

UNIT_MULTIPLIERS = {
    "units": 1,
    "thousands": 1_000,
    "millions": 1_000_000,
    "billions": 1_000_000_000,
}

SHARED_RULES = """
Rules that apply to every field:
- Take the value for the reporting period identified above and no other column. The
  tables usually show several periods side by side (prior year, prior half, full
  year); picking the wrong column is the most common failure here, so verify the
  column header before reading a number.
- For balance sheet items, use the "as at" date of the current reporting period,
  not the comparative.
- Use group / consolidated figures, not segment, parent-company or pro-forma ones.
- Report `value` exactly as printed (do not rescale) and set `unit` to what the
  table header states (S$'000 -> thousands, S$m / S$ million -> millions).
- Numbers in brackets are negative.
- Expenses, interest expense, tax expense and capital expenditure are positive
  numbers. Cash flows keep their natural sign, so an outflow is negative.
- Only compute a value when the report gives you every component on the same
  basis; otherwise return null. Never infer from a prior period or from a
  percentage change.
- If a line item is not present, set value to null, unit to "unknown" and
  confidence to 0.

Confidence rubric (be strict, this gates whether the value is kept at all):
- 0.95-1.0: the exact labelled line item for the correct period, read directly.
- 0.80-0.94: labelled slightly differently but unambiguous, or a clean sum of
  explicitly presented sub-totals.
- 0.70-0.79: correct line item but some doubt about the column or the scale.
- below 0.70: any real doubt about which period, which entity, or which line —
  including anything you had to reason about indirectly. Use this freely.
""".strip()


def compress(text: str) -> str:
    """Strips tokens that carry no information: dot leaders, runs of spaces,
    blank lines, page furniture. Cuts a typical report page by 15-25%."""
    text = re.sub(r"[.…]{4,}", " ", text)  # table-of-contents leaders
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


# Words that mark a line as structurally useful even when it has no figures —
# column headers, unit rows and statement titles.
STATEMENT_VOCAB = re.compile(
    r"(statement|total|net |revenue|turnover|profit|loss|ebit|cash|assets?|"
    r"liabilit|equity|tax|expense|income|capex|capital|dividend|unitholder|"
    r"perpetual|minorit|shares|s\$|us\$|a\$|rm|'000|million|billion|as at|"
    r"half year|full year|financial year|quarter|months ended|ended)",
    re.I,
)


def keep_statement_lines(text: str) -> str:
    """Drops narrative prose, keeps the lines that could hold a figure.

    MD&A pages are mostly commentary wrapped around a couple of tables, and the
    commentary is roughly half the tokens while never being the source of a
    number we report. A line survives if it carries two or more numbers (a table
    row), or is a short line matching the statement vocabulary (a title, column
    header or units row).
    """
    kept = []
    for line in text.splitlines():
        numbers = len(re.findall(r"\d[\d,]*\.?\d*", line))
        if numbers >= 2:
            kept.append(line)
        elif STATEMENT_VOCAB.search(line) and (numbers >= 1 or len(line) < 70):
            kept.append(line)
    return "\n".join(kept)


def numeric_token_count(text: str) -> int:
    return len(re.findall(r"\d", text))


@dataclass
class Page:
    """One PDF page: plain text plus any tables rendered as TSV."""

    number: int  # 1-indexed, matches what the model reports back
    text: str
    tables_text: str = ""
    line_filter: bool = True

    @property
    def content(self) -> str:
        """Compressed text, plus the TSV tables only where they earn their tokens.

        The TSV largely duplicates the text, so it roughly doubles the prompt. It
        is only worth sending on pages that actually hold figures, because that is
        where `extract_text()` mangles the numeric columns.
        """
        body = compress(self.text)
        tables = compress(self.tables_text)
        if self.line_filter:
            body = keep_statement_lines(body)
            tables = keep_statement_lines(tables)
        if tables and numeric_token_count(body) >= MIN_NUMERIC_TOKENS_FOR_TABLES:
            return f"{body}\n[TABLES]\n{tables[:MAX_TABLE_CHARS]}"
        return body

    @property
    def raw_content(self) -> str:
        """Unfiltered text — used for period detection, which reads prose."""
        return compress(self.text)

    @property
    def is_sparse(self) -> bool:
        return len(self.text.strip()) < MIN_TEXT_CHARS_PER_PAGE


@dataclass
class Document:
    path: Path
    pages: list[Page] = field(default_factory=list)

    @property
    def is_scanned(self) -> bool:
        """True when most pages have no usable text layer, so OCR is needed."""
        if not self.pages:
            return True
        sparse = sum(p.is_sparse for p in self.pages)
        return sparse / len(self.pages) > 0.5


def load_pdf(path: Path, line_filter: bool = True) -> Document:
    """Reads every page's text and tables. Table failures are non-fatal."""
    doc = Document(path=path)
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            rows = []
            try:
                for table in page.extract_tables() or []:
                    for row in table:
                        cells = [(c or "").replace("\n", " ").strip() for c in row]
                        if any(cells):
                            rows.append("\t".join(cells))
            except Exception:
                # A malformed table shouldn't cost us the page's text.
                pass
            doc.pages.append(
                Page(
                    number=i,
                    text=text,
                    tables_text="\n".join(rows),
                    line_filter=line_filter,
                )
            )
    return doc


def score_page(page: Page, profile: dict) -> float:
    """Keyword score for one page against one statement-group profile.

    Phrases are weighted 3x terms, and pages dense with numbers get a small bonus
    so prose that merely discusses revenue loses to the table that reports it.
    """
    haystack = page.content.lower()
    if not haystack.strip():
        return 0.0

    score = 0.0
    for phrase in profile["phrases"]:
        score += 3.0 * haystack.count(phrase)
    for term in profile["terms"]:
        score += 1.0 * len(re.findall(rf"\b{re.escape(term)}\b", haystack))

    numeric_tokens = len(re.findall(r"[\d,]+\.?\d*", haystack))
    if numeric_tokens > 30:
        score *= 1.35
    return score


def retrieve_pages(
    doc: Document,
    profile: dict,
    top_k: int = TOP_K_PAGES,
    budget: int = MAX_CHARS_PER_GROUP,
) -> list[Page]:
    """Top-scoring pages plus their immediate neighbours, in document order.

    Statements routinely spill onto the next page (and their unit header sits on
    the previous one), so each hit pulls in +/-1 page.
    """
    scored = [(score_page(p, profile), p) for p in doc.pages]
    scored = [(s, p) for s, p in scored if s > 0]
    if not scored:
        return []

    scored.sort(key=lambda sp: sp[0], reverse=True)
    keep: set[int] = set()
    for _, page in scored[:top_k]:
        keep.update({page.number - 1, page.number, page.number + 1})

    pages = [p for p in doc.pages if p.number in keep]
    return _fit_budget(pages, budget)


def _fit_budget(pages: list[Page], budget: int) -> list[Page]:
    """Drops trailing pages once the character budget is spent."""
    out, used = [], 0
    for page in pages:
        size = len(page.content)
        if used + size > budget and out:
            break
        out.append(page)
        used += size
    return out


def render_pages(pages: list[Page]) -> str:
    return "\n\n".join(f"===== PAGE {p.number} =====\n{p.content}" for p in pages)


def pdf_file_part(path: Path) -> dict:
    """OpenRouter native file part — the fallback when there is no text layer."""
    data = base64.standard_b64encode(path.read_bytes()).decode("utf-8")
    return {
        "type": "file",
        "file": {
            "filename": path.name,
            "file_data": f"data:application/pdf;base64,{data}",
        },
    }


def nullable(schema: dict) -> dict:
    """`anyOf`-style optional field — strict JSON-schema mode rejects type arrays."""
    return {"anyOf": [schema, {"type": "null"}]}


PERIOD_SCHEMA = {
    "type": "object",
    "properties": {
        "company_name": nullable({"type": "string"}),
        "period_type": {
            "type": "string",
            "enum": ["full_year", "half_year", "quarter", "nine_month", "other"],
            "description": "The most recent income-statement period this report is about.",
        },
        "period_label": {
            "type": "string",
            "description": "How that period is written in the tables, e.g. 'Sep 25', 'H1 FY26', '2H FY2026', 'FY2026'.",
        },
        "period_end": nullable(
            {"type": "string", "description": "Period end date as YYYY-MM-DD."}
        ),
        "period_start": nullable({"type": "string"}),
        "balance_sheet_date": nullable(
            {
                "type": "string",
                "description": "The 'as at' date of the current balance sheet, YYYY-MM-DD.",
            }
        ),
        "currency": nullable({"type": "string", "description": "ISO code, e.g. SGD."}),
        "is_audited": nullable({"type": "boolean"}),
        "column_guidance": {
            "type": "string",
            "description": "One or two sentences telling a later reader exactly which column of the tables to read, and which columns to avoid.",
        },
        "confidence": {"type": "number"},
    },
    "required": [
        "company_name", "period_type", "period_label", "period_end", "period_start",
        "balance_sheet_date", "currency", "is_audited", "column_guidance", "confidence",
    ],
    "additionalProperties": False,
}

PERIOD_PROMPT = """
You are reading an SGX-listed issuer's periodic financial report.

Identify the single reporting period the document is about — the most recent one it
presents, at the granularity it reports on. If the report covers a half year, that is
half_year. If it covers a full financial year, that is full_year, even when it also
shows the second half separately. If it covers a quarter, that is quarter.

Then describe how that period's column is labelled in the financial tables, and which
neighbouring columns (prior year, prior half, full year vs half year) must NOT be read.
Be concrete — a later step will use `column_guidance` verbatim to pick columns.

Return JSON only.
""".strip()


MONTHS = {
    m: i
    for i, m in enumerate(
        ["january", "february", "march", "april", "may", "june", "july", "august",
         "september", "october", "november", "december"],
        start=1,
    )
}

# "FOR THE HALF YEAR ENDED 30 SEPTEMBER 2025",
# "FOR THE SECOND HALF AND FINANCIAL YEAR ENDED 31 MARCH 2026", etc.
PERIOD_RE = re.compile(
    r"for the\s+(?P<phrase>[A-Za-z \-]{0,60}?)\s*ended[,\s]+"
    r"(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})",
    re.I,
)


# Currency as it is printed in statement column headers ("S$'000", "US$ Mil",
# "RM'000"). Ordered longest-first so US$ is not read as S$.
CURRENCY_PATTERNS = [
    (r"\bUS\s?\$|\bUSD\b", "USD"),
    (r"\bS\s?\$|\bSGD\b|\bSing(?:apore)?\s+dollar", "SGD"),
    (r"\bHK\s?\$|\bHKD\b", "HKD"),
    (r"\bA\s?\$|\bAUD\b", "AUD"),
    (r"\bNZ\s?\$|\bNZD\b", "NZD"),
    (r"\bRMB\b|\bCNY\b|\bRenminbi\b", "CNY"),
    (r"\bRM\b|\bMYR\b|\bRinggit\b", "MYR"),
    (r"\bIDR\b|\bRp\b|\bRupiah\b", "IDR"),
    (r"\bTHB\b|\bBaht\b", "THB"),
    (r"\bJPY\b|\bYen\b", "JPY"),
    (r"\bEUR\b|\bEuro\b|€", "EUR"),
    (r"\bGBP\b|\bSterling\b|£", "GBP"),
    (r"\bINR\b|\bRupee\b", "INR"),
]


MONTH_ABBR = {
    m: i
    for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun",
         "jul", "aug", "sep", "oct", "nov", "dec"],
        start=1,
    )
}


def parse_date(text: str | None) -> tuple[int, int, int] | None:
    """(year, month, day) from the date forms these reports and models produce.

    The local cover-page rule emits ISO, but the model returns whatever the
    document prints — "31 March 2026", "December 31, 2025", "31/03/2026".
    """
    if not text or not isinstance(text, str):
        return None
    # Reports write ordinals ("30th June 2026"); drop the suffix before parsing.
    text = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", text.strip(), flags=re.I)

    iso = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if iso:
        return int(iso.group(1)), int(iso.group(2)), int(iso.group(3))

    # "31 March 2026" / "31 Mar 2026"
    dmy = re.match(r"(\d{1,2})\s+([A-Za-z]+),?\s+(\d{4})", text)
    if dmy and dmy.group(2).lower()[:3] in MONTH_ABBR:
        return int(dmy.group(3)), MONTH_ABBR[dmy.group(2).lower()[:3]], int(dmy.group(1))

    # "December 31, 2025" / "Dec 31 2025"
    mdy = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if mdy and mdy.group(1).lower()[:3] in MONTH_ABBR:
        return int(mdy.group(3)), MONTH_ABBR[mdy.group(1).lower()[:3]], int(mdy.group(2))

    # "31/03/2026" — day first, as SGX reports are not US-formatted.
    dmy_slash = re.match(r"(\d{1,2})[/.](\d{1,2})[/.](\d{4})", text)
    if dmy_slash:
        return (int(dmy_slash.group(3)), int(dmy_slash.group(2)),
                int(dmy_slash.group(1)))
    return None


def iso_date(text: str | None) -> str | None:
    """Normalises any recognised date form to YYYY-MM-DD, else returns it as-is."""
    parsed = parse_date(text)
    if not parsed:
        return text
    return "{:04d}-{:02d}-{:02d}".format(*parsed)


# The scale token that follows a currency in a statement's unit row. Issuers
# write these as "S$'000", "$'M", "US$ million", "RM'000" — hence the optional
# apostrophe before the abbreviated scales.
UNIT_TAIL = r"\s*'?(?:000|m\b|mil\b|million|bn\b|billion)"
# Loose symbols are only trusted as a last resort, and only in bulk: a report can
# mention a foreign currency once in a note while reporting in another.
MIN_LOOSE_CURRENCY_HITS = 5


def normalise_currency(value: str | None) -> str | None:
    """Maps whatever the model reports ("US$", "S$ million", "Singapore dollar")
    to an ISO code, so the FX table can be keyed on it."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    # Three-letter codes that aren't the ISO one the FX table is keyed on.
    aliases = {"RMB": "CNY", "SGP": "SGD", "US$": "USD"}
    if re.fullmatch(r"[A-Z]{3}", text.upper()):
        return aliases.get(text.upper(), text.upper())
    for pattern, code in CURRENCY_PATTERNS:
        if re.search(pattern, text, re.I):
            return code
    return None


def sniff_currency(doc: Document) -> str | None:
    """Reads the reporting currency off the statement unit rows ("S$'000").

    The model is asked for this too, but the cover-page fast path skips that
    call, and conversion to SGD downstream needs it on every report.

    Only unit rows are counted, not loose mentions: Keppel REIT's text says "A$"
    once while discussing Australian assets but reports in Singapore dollars, so
    counting bare symbols anywhere in the document picks the wrong currency.
    Issuers reporting in SGD often print an unprefixed "$'000", which is treated
    as SGD — the default for an SGX-listed issuer.
    """
    # Curly apostrophes are common in these headers; normalise before matching.
    text = " ".join(p.raw_content for p in doc.pages).replace("’", "'")

    counts = {}
    for pattern, code in CURRENCY_PATTERNS:
        # The patterns contain alternations, so group them before appending the
        # unit tail — otherwise "A$393.8 million" matches on the bare symbol.
        hits = len(re.findall(f"(?:{pattern}){UNIT_TAIL}", text, re.I))
        if hits:
            counts[code] = counts.get(code, 0) + hits
    if counts:
        return max(counts, key=counts.get)

    # No prefixed currency in a unit row — fall back to a bare "$'000" / "$'M"
    # header, which for an SGX-listed issuer means Singapore dollars.
    if re.search(r"(?<![A-Za-z])\$" + UNIT_TAIL, text):
        return "SGD"

    # Some issuers print no unit row at all and prefix every figure instead
    # ("S$1,234"). Fall back to the dominant symbol, but only when it appears
    # often enough that a passing mention in a note can't win.
    loose = {}
    for pattern, code in CURRENCY_PATTERNS:
        hits = len(re.findall(f"(?:{pattern})", text, re.I))
        if hits >= MIN_LOOSE_CURRENCY_HITS:
            loose[code] = loose.get(code, 0) + hits
    if loose:
        return max(loose, key=loose.get)
    return None


def as_second_half(period: dict) -> dict:
    """Re-points a full-year period at the report's second-half column.

    A full-year filing from a half-yearly reporter shows both periods side by
    side, so this only changes which column to read — the balance sheet keeps the
    year-end "as at" date, because a half year has no separate balance sheet.
    """
    # The local rule emits ISO dates, but the model path returns whatever the
    # report prints ("31 March 2026", "December 31, 2025"), so parse leniently
    # and simply omit the start date when it can't be read.
    end = period.get("period_end")
    start = None
    parsed = parse_date(end)
    if parsed:
        year, month, _ = parsed
        start_month = month - 5
        start_year = year + (start_month - 1) // 12
        start_month = (start_month - 1) % 12 + 1
        start = f"{start_year:04d}-{start_month:02d}-01"

    return {
        **period,
        "period_type": "half_year",
        "period_basis": "second_half_of_full_year_report",
        "period_label": f"second half ended {end}" if end else "second half",
        "period_start": start,
        # balance_sheet_date is deliberately left as the year-end date.
        "column_guidance": (
            "This is a full-year report, but extract ONLY the second-half column. "
            "It is usually headed 'Second Half Year', '2H', '2H 2025', 'Half Year "
            "ended ...' or 'Six months ended ...' and sits beside the 12-month "
            "column. Do NOT read the full-year / 'financial year ended' column, and "
            "do NOT read any prior-year column (e.g. 2H of the previous year). "
            "If a statement shows no second-half column at all — cash flow "
            "statements are often presented for the full year only — return null "
            "for those fields rather than substituting the full-year figure. "
            "Balance sheet items are the exception: use the year-end 'as at' "
            "column as normal, since a half year has no separate balance sheet."
        ),
    }


def detect_period_locally(doc: Document) -> dict | None:
    """Reads the period straight off the cover page, skipping an API call.

    SGX cover pages state the period in a fixed idiom, so the common case needs no
    model at all. Anything unusual returns None and falls through to the model.
    """
    head = "\n".join(p.raw_content for p in doc.pages[:3])
    match = PERIOD_RE.search(head)
    if not match:
        return None

    phrase = match.group("phrase").lower()
    month = MONTHS.get(match.group("month").lower())
    if month is None:
        return None

    # "second half AND financial year ended" is a full-year report that also shows
    # the half, so check for the year wording before the half wording.
    if re.search(r"financial year|full year|fiscal year|^year", phrase.strip()):
        period_type, months_long = "full_year", 12
    elif re.search(r"half year|half-year|six months|first half|second half", phrase):
        period_type, months_long = "half_year", 6
    elif re.search(r"quarter|three months", phrase):
        period_type, months_long = "quarter", 3
    elif re.search(r"nine months", phrase):
        period_type, months_long = "nine_month", 9
    else:
        return None  # "period ended", "financial period" — let the model decide

    day, year = int(match.group("day")), int(match.group("year"))
    end = f"{year:04d}-{month:02d}-{day:02d}"
    start_month = month - months_long + 1
    start_year = year + (start_month - 1) // 12
    start_month = (start_month - 1) % 12 + 1
    label = f"{match.group('month')[:3].title()} {str(year)[2:]}"

    return {
        "company_name": None,
        "period_type": period_type,
        "period_label": label,
        "period_end": end,
        "period_start": f"{start_year:04d}-{start_month:02d}-01",
        "balance_sheet_date": end,
        "currency": None,
        "is_audited": None,
        "column_guidance": (
            f"Read only the column for the {period_type.replace('_', ' ')} ended "
            f"{day} {match.group('month').title()} {year} (often labelled "
            f"'{label}'). Ignore prior-year and prior-period comparatives, and "
            f"ignore columns for any other period length."
        ),
        "confidence": 0.9,
        "detected_by": "cover-page rule",
    }


def detect_period(
    client: OpenAI,
    doc: Document,
    model: str,
    pdf_model: str,
    context_chars: int = MAX_CHARS_PER_GROUP,
    usage: dict | None = None,
    full_year_basis: str = FULL_YEAR_BASIS,
) -> dict:
    """Works out what 'the most recent period' means for this specific document."""
    second_half = full_year_basis == "second_half"

    local = detect_period_locally(doc)
    if local:
        return as_second_half(local) if second_half and local["period_type"] == "full_year" else local

    if doc.is_scanned:
        content = [pdf_file_part(doc.path), {"type": "text", "text": PERIOD_PROMPT}]
        detected = call_model(
            client, pdf_model, content, PERIOD_SCHEMA, "period", pdf=True, usage=usage
        )
        return (
            as_second_half(detected)
            if second_half and detected.get("period_type") == "full_year"
            else detected
        )

    # Cover pages carry the period; highlights pages carry the column headers.
    # Half the group budget is plenty — this only needs headers, not line items.
    head = doc.pages[:3]
    profile = GROUP_QUERIES["income_statement"]
    extra = [p for p in retrieve_pages(doc, profile, top_k=2) if p not in head]
    pages = _fit_budget(head + extra, context_chars // 2)
    content = [{"type": "text", "text": f"{PERIOD_PROMPT}\n\n{render_pages(pages)}"}]
    detected = call_model(client, model, content, PERIOD_SCHEMA, "period", usage=usage)
    return (
        as_second_half(detected)
        if second_half and detected.get("period_type") == "full_year"
        else detected
    )


def build_group_schema(fields: list[str]) -> dict:
    """One object per metric: value + unit + page + confidence."""
    item = {
        "type": "object",
        "properties": {
            "value": nullable({"type": "number"}),
            "unit": {
                "type": "string",
                "enum": ["units", "thousands", "millions", "billions", "unknown"],
                "description": "Scale printed in the table header. 'unknown' only when value is null.",
            },
            "source_page": nullable({"type": "integer"}),
            "source_label": nullable(
                {
                    "type": "string",
                    "description": "The line-item label as printed in the report.",
                }
            ),
            "confidence": {"type": "number"},
        },
        "required": ["value", "unit", "source_page", "source_label", "confidence"],
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "properties": {f: dict(item) for f in fields},
        "required": list(fields),
        "additionalProperties": False,
    }


def build_group_prompt(group: str, fields: list[str], period: dict) -> str:
    hints = "\n".join(f"- {f}: {FIELD_HINTS.get(f, '')}".rstrip() for f in fields)
    return f"""
You are extracting {group.replace('_', ' ')} figures from an SGX-listed issuer's
periodic financial report.

Reporting period to extract:
- period type: {period.get('period_type')}
- period label in the tables: {period.get('period_label')}
- period: {period.get('period_start')} to {period.get('period_end')}
- balance sheet as at: {period.get('balance_sheet_date')}
- currency: {period.get('currency')}
- how to pick the column: {period.get('column_guidance')}

Fields to extract:
{hints}

{SHARED_RULES}

Return JSON only.
""".strip()


def extract_group(
    client: OpenAI,
    doc: Document,
    group: str,
    fields: list[str],
    period: dict,
    model: str,
    pdf_model: str,
    context_chars: int = MAX_CHARS_PER_GROUP,
    usage: dict | None = None,
) -> dict:
    prompt = build_group_prompt(group, fields, period)
    schema = build_group_schema(fields)
    pages = (
        []
        if doc.is_scanned
        else retrieve_pages(doc, GROUP_QUERIES[group], budget=context_chars)
    )

    if pages:
        content = [{"type": "text", "text": f"{prompt}\n\n{render_pages(pages)}"}]
        return call_model(client, model, content, schema, group, usage=usage)

    # Nothing retrieved (or no text layer at all) — let the model read the PDF.
    content = [pdf_file_part(doc.path), {"type": "text", "text": prompt}]
    return call_model(client, pdf_model, content, schema, group, pdf=True, usage=usage)


class InsufficientCredits(RuntimeError):
    """OpenRouter balance can't cover the request even after shrinking it."""


def output_budget(schema: dict) -> int:
    """Right-sizes max_tokens from the number of fields the schema asks for."""
    fields = len(schema.get("properties", {}))
    return fields * TOKENS_PER_FIELD + OUTPUT_TOKEN_OVERHEAD


def affordable_tokens(error: APIStatusError) -> int | None:
    """Pulls 'can only afford N' out of a 402 body so we can retry inside budget."""
    match = re.search(r"can only afford (\d+)", str(error))
    return int(match.group(1)) if match else None


def extract_all_groups(
    client: OpenAI,
    doc: Document,
    metrics: dict,
    period: dict,
    model: str,
    pdf_model: str,
    context_chars: int = MAX_CHARS_TOTAL,
    usage: dict | None = None,
    reasoning: bool = False,
) -> dict:
    """Every metric in one call, over the union of each group's retrieved pages.

    The three groups overlap heavily — the appendix pages that hold the balance
    sheet usually hold the cash flow statement too — so sending the union once is
    cheaper than sending three overlapping prompts, and the shared rules and
    period preamble are paid for once instead of three times.
    """
    all_fields = [f for spec in metrics.values() for f in spec["fields"]]
    schema = build_group_schema(all_fields)
    prompt = build_group_prompt("financial statement", all_fields, period)

    union: dict[int, Page] = {}
    if not doc.is_scanned:
        for group in metrics:
            profile = GROUP_QUERIES.get(group, GROUP_QUERIES["income_statement"])
            for page in retrieve_pages(doc, profile, budget=context_chars):
                union[page.number] = page
    pages = _fit_budget([union[n] for n in sorted(union)], context_chars)

    if pages:
        content = [{"type": "text", "text": f"{prompt}\n\n{render_pages(pages)}"}]
        chars = sum(len(p.content) for p in pages)
        log(f"retrieved {len(pages)} pages, ~{chars // 4:,} prompt tokens")
        raw = call_model(
            client, model, content, schema, "financials",
            usage=usage, reasoning=reasoning,
        )
    else:
        log("no text layer — sending the PDF itself for OCR (slower)")
        content = [pdf_file_part(doc.path), {"type": "text", "text": prompt}]
        raw = call_model(
            client, pdf_model, content, schema, "financials",
            pdf=True, usage=usage, reasoning=reasoning,
        )

    # Split the flat answer back into the metrics file's groups.
    return {
        group: {f: raw.get(f) for f in spec["fields"]}
        for group, spec in metrics.items()
    }


def call_model(
    client: OpenAI,
    model: str,
    content: list,
    schema: dict,
    schema_name: str,
    pdf: bool = False,
    usage: dict | None = None,
    reasoning: bool = False,
) -> dict:
    """One JSON-constrained call, with a fallback ladder.

    OpenRouter serves a model from several providers and not all of them honour
    `response_format` or the unified `reasoning` field, so a 400 is expected
    rather than exceptional: retry without reasoning, then in plain JSON mode with
    the schema inlined in the prompt. A 402 is handled separately — OpenRouter
    reports what the remaining balance can afford, so the call is retried once
    inside that budget before giving up.
    """
    messages = [{"role": "user", "content": content}]
    base = {
        "model": model,
        "messages": messages,
        "max_tokens": output_budget(schema),
        "temperature": 0,
    }
    strict_format = {
        "type": "json_schema",
        "json_schema": {"name": schema_name, "strict": True, "schema": schema},
    }
    # require_parameters keeps OpenRouter off providers that would silently ignore
    # the schema; the plugin only applies when we send the PDF itself.
    extra: dict = {"provider": {"require_parameters": True}}
    if pdf:
        extra["plugins"] = [{"id": "file-parser", "pdf": {"engine": PDF_ENGINE}}]

    # Reasoning is deliberately off: this is table lookup, not inference, and a
    # chain-of-thought pass over a 33-field schema costs minutes and tokens for
    # no accuracy gain. `--reasoning` puts it back on the first attempt.
    first_extra = {**extra, "reasoning": {"effort": "medium"}} if reasoning else extra
    attempts = [
        {**base, "response_format": strict_format, "extra_body": first_extra},
        {**base, "response_format": strict_format, "extra_body": extra},
        {**base, "response_format": {"type": "json_object"},
         "extra_body": {k: v for k, v in extra.items() if k != "provider"}},
    ]

    last_error: Exception | None = None
    for i, kwargs in enumerate(attempts):
        if i == len(attempts) - 1:
            # Plain JSON mode has no schema enforcement, so state it in the prompt.
            kwargs = dict(kwargs)
            kwargs["messages"] = _with_schema_in_prompt(messages, schema)
        label = kwargs["response_format"]["type"]
        log(
            f"calling {model} [{schema_name}, {label}, "
            f"max_tokens={kwargs['max_tokens']}] attempt {i + 1}/{len(attempts)}"
        )
        try:
            response = _create_within_budget(client, kwargs)
            _record_usage(usage, response)
            result = parse_json(response.choices[0].message.content)
            log(f"got {schema_name}")
            return result
        except APITimeoutError as exc:
            log(f"timed out after {REQUEST_TIMEOUT:.0f}s — trying next attempt")
            last_error = exc
        except (BadRequestError, ValueError) as exc:
            log(f"attempt {i + 1} rejected ({type(exc).__name__}) — trying next")
            last_error = exc

    if isinstance(last_error, APITimeoutError):
        raise RuntimeError(
            f"{model} timed out on every attempt. The provider OpenRouter routed to "
            f"is slow or stalled: retry (routing may pick another), raise "
            f"--timeout, cut --context-chars, or switch --model."
        ) from last_error
    raise RuntimeError(f"{model} could not return valid JSON for {schema_name}: {last_error}")


def _create_within_budget(client: OpenAI, kwargs: dict):
    """Sends the request, refitting max_tokens once if the balance is too small."""
    try:
        return client.chat.completions.create(**kwargs)
    except APIStatusError as exc:
        if exc.status_code != 402:
            raise
        afford = affordable_tokens(exc)
        if afford is None or afford >= kwargs["max_tokens"]:
            # Shrinking the reply won't help — it's the prompt that's unaffordable.
            raise InsufficientCredits(
                "OpenRouter balance is too low even for this prompt. Shrink it with "
                "--context-chars (e.g. 12000), switch to a cheaper --model "
                "(moonshotai/kimi-k2-thinking is the cheapest Kimi), or add credits "
                "at https://openrouter.ai/settings/credits."
            ) from exc
        if afford < MIN_USEFUL_OUTPUT_TOKENS:
            raise InsufficientCredits(
                f"OpenRouter balance affords only {afford} output tokens, which is "
                f"too few to answer. Add credits at "
                f"https://openrouter.ai/settings/credits, or cut the prompt with "
                f"--context-chars and retry with a cheaper --model."
            ) from exc
        # Leave a little headroom; the quote moves with the prompt's own cost.
        kwargs = {**kwargs, "max_tokens": afford - 50}
        return client.chat.completions.create(**kwargs)


def _usage_cost(usage_obj) -> float:
    """The credits OpenRouter actually charged for one call.

    OpenRouter adds `cost` to the usage object on every response. It is not part
    of the OpenAI schema, so depending on SDK version it lands either on the model
    itself or in the extras bag — check both rather than guessing.
    """
    cost = getattr(usage_obj, "cost", None)
    if cost is None:
        extra = getattr(usage_obj, "model_extra", None) or {}
        cost = extra.get("cost")
    try:
        return float(cost)
    except (TypeError, ValueError):
        return 0.0


def _record_usage(usage: dict | None, response) -> None:
    """Accumulates tokens and cost so a run can report what it spent."""
    if usage is None or not getattr(response, "usage", None):
        return
    cost = _usage_cost(response.usage)
    prompt = response.usage.prompt_tokens or 0
    completion = response.usage.completion_tokens or 0

    usage["calls"] = usage.get("calls", 0) + 1
    usage["prompt_tokens"] = usage.get("prompt_tokens", 0) + prompt
    usage["completion_tokens"] = usage.get("completion_tokens", 0) + completion
    usage["cost_usd"] = round(usage.get("cost_usd", 0.0) + cost, 6)
    usage.setdefault("per_call", []).append(
        {
            "model": getattr(response, "model", None),
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "cost_usd": round(cost, 6),
        }
    )
    log(f"  {prompt:,} prompt + {completion:,} completion tokens, ${cost:.5f}")


def _with_schema_in_prompt(messages: list, schema: dict) -> list:
    parts = list(messages[0]["content"])
    parts.append(
        {
            "type": "text",
            "text": "Reply with a single JSON object matching this schema exactly:\n"
            + json.dumps(schema),
        }
    )
    return [{"role": "user", "content": parts}]


def parse_json(text: str | None) -> dict:
    """Lenient parse — some providers wrap JSON in prose or a ```json fence."""
    if not text:
        raise ValueError("empty response")
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.S)
    if fence:
        text = fence.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError(f"no JSON object in response: {text[:200]!r}")
    return json.loads(text[start : end + 1])


def apply_confidence_gate(raw: dict, min_confidence: float) -> tuple[dict, dict]:
    """Splits an extracted group into final values and per-field provenance.

    Anything the model wasn't at least `min_confidence` sure of becomes null, and
    values are scaled from their printed unit into absolute currency units. An
    unresolved unit is treated as a miss too — an unscalable number is worse than
    no number.
    """
    values, details = {}, {}
    for name, item in raw.items():
        item = item or {}
        try:
            confidence = float(item.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        value = item.get("value")
        value = value if isinstance(value, (int, float)) else None
        unit = item.get("unit") or "unknown"
        kept = (
            value is not None
            and confidence >= min_confidence
            and unit in UNIT_MULTIPLIERS
        )

        scaled = value * UNIT_MULTIPLIERS[unit] if kept else None
        values[name] = scaled
        details[name] = {
            "value_reported": value,
            "unit": unit if value is not None else None,
            "value": scaled,
            "confidence": round(confidence, 2),
            "kept": kept,
            "source_page": item.get("source_page"),
            "source_label": item.get("source_label"),
        }
    return values, details


START = time.monotonic()


def log(message: str) -> None:
    """Stage progress on stderr, so a slow run shows where the time is going."""
    print(f"[{time.monotonic() - START:6.1f}s] {message}", file=sys.stderr, flush=True)


def mirror_ebit_and_operating_income(values: dict, details: dict) -> None:
    """Forces `ebit` and `operating_income` to carry the same figure.

    Issuers present one or the other, rarely both: Singtel's income statement has
    an EBIT line and no operating profit line, Jardine's has operating profit and
    no EBIT. Treating them as one metric fills whichever side is missing. When
    both are present `operating_income` wins, so a broader EBIT (one that folds in
    associates' pre-tax profits, say) does not silently redefine the field.

    Mutates the income-statement group in place; a copied value is marked in
    provenance with `derived_from` so it is never mistaken for a printed line.
    """
    income = values.get("income_statement")
    if income is None or "ebit" not in income or "operating_income" not in income:
        return
    prov = details["income_statement"]

    source = (
        "operating_income" if income.get("operating_income") is not None else "ebit"
    )
    target = "ebit" if source == "operating_income" else "operating_income"
    if income.get(source) is None:
        return  # neither side was extracted; nothing to mirror
    if income.get(target) == income[source]:
        return  # already equal

    income[target] = income[source]
    prov[target] = {
        **prov[source],
        "derived_from": source,
        "replaced_value": prov[target].get("value"),
        "replaced_source_label": prov[target].get("source_label"),
    }


def build_client(
    api_key: str | None = None, timeout: float = REQUEST_TIMEOUT
) -> OpenAI:
    """OpenRouter speaks the OpenAI API, so the OpenAI SDK is the client."""
    load_dotenv()
    key = api_key or os.getenv("OPENROUTER_API_KEY")
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY is not set (env or .env)")
    # Optional attribution headers; OpenRouter ignores them when unset.
    headers = {}
    if os.getenv("OPENROUTER_SITE_URL"):
        headers["HTTP-Referer"] = os.environ["OPENROUTER_SITE_URL"]
    if os.getenv("OPENROUTER_APP_NAME"):
        headers["X-Title"] = os.environ["OPENROUTER_APP_NAME"]
    return OpenAI(
        base_url=OPENROUTER_BASE_URL,
        api_key=key,
        default_headers=headers,
        timeout=timeout,
        # The SDK retries twice by default; combined with our own fallback ladder
        # that is up to 9 attempts of a slow call before anything surfaces.
        max_retries=0,
    )


def extract_financials(
    pdf_path: Path,
    metrics_file: Path = METRICS_FILE,
    model: str = MODEL,
    pdf_model: str = PDF_MODEL,
    min_confidence: float = MIN_CONFIDENCE,
    context_chars: int | None = None,
    per_group_calls: bool = False,
    line_filter: bool = True,
    reasoning: bool = False,
    full_year_basis: str = FULL_YEAR_BASIS,
    client: OpenAI | None = None,
) -> dict:
    """Runs the whole pipeline over one PDF and returns values + provenance."""
    metrics = json.loads(Path(metrics_file).read_text())
    client = client or build_client()
    log(f"parsing {Path(pdf_path).name}")
    doc = load_pdf(Path(pdf_path), line_filter=line_filter)
    if not doc.pages:
        raise ValueError(f"no pages read from {pdf_path}")
    log(f"parsed {len(doc.pages)} pages")

    if context_chars is None:
        context_chars = MAX_CHARS_PER_GROUP if per_group_calls else MAX_CHARS_TOTAL

    usage: dict = {}
    period = detect_period(
        client, doc, model, pdf_model, context_chars, usage, full_year_basis
    )
    # The model returns dates as the report prints them; downstream consumers
    # (the half-yearly table, FX lookup) need them comparable.
    for key in ("period_end", "period_start", "balance_sheet_date"):
        period[key] = iso_date(period.get(key))

    if not period.get("currency"):
        period["currency"] = sniff_currency(doc)
        period["currency_detected_by"] = "header sniff"
    period["currency"] = normalise_currency(period.get("currency"))
    log(
        f"period: {period['period_type']} {period['period_label']} "
        f"({period.get('currency')}) (via {period.get('detected_by', 'model')})"
        + (" [second-half basis]" if period.get("period_basis") else "")
    )

    values, details = {}, {}
    if per_group_calls:
        for group, spec in metrics.items():
            if group not in GROUP_QUERIES:
                # An unknown group in metrics_scraped.json still gets extracted; it
                # just falls back to the income-statement retrieval profile.
                GROUP_QUERIES[group] = GROUP_QUERIES["income_statement"]
            raw = extract_group(
                client, doc, group, spec["fields"], period, model, pdf_model,
                context_chars, usage,
            )
            values[group], details[group] = apply_confidence_gate(raw, min_confidence)
    else:
        for group, raw in extract_all_groups(
            client, doc, metrics, period, model, pdf_model, context_chars, usage,
            reasoning,
        ).items():
            values[group], details[group] = apply_confidence_gate(raw, min_confidence)

    mirror_ebit_and_operating_income(values, details)

    return {
        "source_file": Path(pdf_path).name,
        "page_count": len(doc.pages),
        "text_layer": "missing" if doc.is_scanned else "present",
        "model": pdf_model if doc.is_scanned else model,
        "period": period,
        "min_confidence": min_confidence,
        "usage": usage,
        "metrics": values,
        "provenance": details,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("pdf", type=Path, help="path to the report PDF")
    parser.add_argument("-o", "--out", type=Path, help="write JSON here instead of stdout")
    parser.add_argument("--model", default=MODEL, help=f"OpenRouter model (default {MODEL})")
    parser.add_argument(
        "--pdf-model",
        default=PDF_MODEL,
        help=f"model used when the PDF has no text layer (default {PDF_MODEL})",
    )
    parser.add_argument("--metrics", type=Path, default=METRICS_FILE)
    parser.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
    parser.add_argument(
        "--context-chars",
        type=int,
        default=None,
        help=f"prompt character budget (default {MAX_CHARS_TOTAL} merged, "
        f"{MAX_CHARS_PER_GROUP} per group); lower it to cut token spend",
    )
    parser.add_argument(
        "--per-group-calls",
        action="store_true",
        help="one call per statement group instead of one merged call — ~2x the "
        "cost, use if the merged prompt is losing accuracy",
    )
    parser.add_argument(
        "--full-pages",
        action="store_true",
        help="send whole pages instead of only figure-bearing lines (more tokens)",
    )
    parser.add_argument(
        "--reasoning",
        action="store_true",
        help="let the model think before answering — slower and dearer, only worth "
        "it if figures are being misread",
    )
    parser.add_argument(
        "--full-year-basis",
        choices=["second_half", "full_year"],
        default=FULL_YEAR_BASIS,
        help=f"what to extract from a full-year report (default {FULL_YEAR_BASIS}): "
        "the second-half column, or the 12-month column",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=REQUEST_TIMEOUT,
        help=f"seconds before a request is abandoned (default {REQUEST_TIMEOUT:.0f})",
    )
    args = parser.parse_args()

    try:
        client = build_client(timeout=args.timeout)
        result = extract_financials(
            args.pdf,
            metrics_file=args.metrics,
            model=args.model,
            pdf_model=args.pdf_model,
            min_confidence=args.min_confidence,
            context_chars=args.context_chars,
            per_group_calls=args.per_group_calls,
            line_filter=not args.full_pages,
            reasoning=args.reasoning,
            full_year_basis=args.full_year_basis,
            client=client,
        )
    except (RuntimeError, InsufficientCredits) as exc:
        print(exc, file=sys.stderr)
        return 1

    payload = json.dumps(result, indent=2)
    if args.out:
        args.out.write_text(payload)
        kept = sum(v is not None for g in result["metrics"].values() for v in g.values())
        total = sum(len(g) for g in result["metrics"].values())
        used = result["usage"]
        print(
            f"{args.pdf.name}: {kept}/{total} metrics extracted -> {args.out}\n"
            f"{used.get('calls', 0)} calls, "
            f"{used.get('prompt_tokens', 0):,} prompt + "
            f"{used.get('completion_tokens', 0):,} completion tokens, "
            f"cost ${used.get('cost_usd', 0.0):.5f}"
        )
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
