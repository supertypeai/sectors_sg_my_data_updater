from datetime import date, datetime, timedelta
from io import BytesIO

import logging
import openpyxl
import re


LOGGER = logging.getLogger(__name__)

INSTITUTIONAL = "institutional"
RETAIL = "retail"

SECTOR_SHEETS = {"institutional": INSTITUTIONAL, "retail": RETAIL}

TOP_10_SHEET = "weekly top 10"
STI_SHEET = "sti constituents"

# Every amount in the workbook is in S$ millions
MILLION = 1_000_000

OVERALL = "Overall"

# "Week of 16 November 2026", "Week of 9 Feb2026", "Week of 11 to 14 December 2018"
WEEK_PATTERN = re.compile(
    r"week\s+of\s+(\d{1,2})(?:\s*(?:to|-)\s*\d{1,2})?\s*([a-z]+)\s*(\d{4})", re.IGNORECASE
)

NET_BUY = "net_buy"
NET_SELL = "net_sell"

# SGX's sector names, its own since Aug 2019 and GICS before, mapped to the
# sectors in sgx_company_report. Only the names are mapped, the figures are
# SGX's totals over its own grouping. Transportation & Logistics and
# Infrastructures have no SGX sector, SGX counts them in Industrials
SECTORS = {
    # SGX classification
    "Consumer Cyclicals": "Consumer Cyclicals",
    "Cyclicals Consumer": "Consumer Cyclicals",
    "Consumer Non-Cyclicals": "Consumer Defensive",
    "Energy/Oil & Gas": "Energy",
    "Financial Services": "Financial Services",
    "Health care": "Healthcare",
    "Healthcare": "Healthcare",
    "Industrials": "Industrials",
    "Materials & Resources": "Basic Materials",
    "Real Estate (excl. REITs)": "Properties & Real Estate",
    "REITs": "REIT",
    "Technology (Hardware/Software)": "Technology",
    # SGX's telcos only, ours also holds media, the closest of our sectors
    "Telcos": "Communication Services",
    "Telecommunications": "Communication Services",
    # GICS, 2018 to Aug 2019
    "Consumer Discretionary": "Consumer Cyclicals",
    "Consumer Staples": "Consumer Defensive",
    "Energy": "Energy",
    "Financials": "Financial Services",
    "Health Care": "Healthcare",
    "Information Technology": "Technology",
    "Materials": "Basic Materials",
    "Comm. Services": "Communication Services",
    "Communication Services": "Communication Services",
    "Telecommunication Services": "Communication Services",
    "Utilities": "Utilities",
}


class WorkbookLayoutError(ValueError):
    """The workbook no longer has a table where the parser expects one"""


def clean_text(value) -> str:
    # Sector headers wrap with newlines and REITs carried a footnote star
    return re.sub(r"\s+", " ", str(value or "").replace("*", "")).replace("/ ", "/").strip()


def to_sgd(value) -> int | None:
    if not isinstance(value, (int, float)):
        return None

    return round(value * MILLION)


def monday(day: date) -> date:
    # A week after a Monday holiday is printed from the Tuesday
    return day - timedelta(days=day.weekday())


def parse_week(text: str) -> date | None:
    match = WEEK_PATTERN.search(text or "")

    if not match:
        return None

    day, month, year = match.groups()

    # Months are written in full or cut short, "Sept" included, so the first
    # three letters are enough
    try:
        return datetime.strptime(f"{day} {month[:3]} {year}", "%d %b %Y").date()

    except ValueError:
        return None


def to_symbol(code) -> str | None:
    code = clean_text(code).upper()
    return f"{code}.SI" if code else None


def sheet_rows(sheet) -> list[list]:
    return [list(row) for row in sheet.iter_rows(values_only=True)]


def find_sheet(workbook, name: str):
    for sheet in workbook.worksheets:
        if sheet.title.strip().lower() == name:
            return sheet

    return None


def parse_sector_sheet(sheet, investor_type: str, week: date) -> list[dict]:
    """
    The sheet's last row is the workbook's week and each row above it one week
    earlier. The printed dates are only checked, a few carry typos (a Thursday,
    or the next week's Monday)
    """
    rows = sheet_rows(sheet)

    # The header row names the sectors, column B reads "... net buy/sell (S$M)"
    header_index = next(
        (
            index
            for index, row in enumerate(rows)
            if len(row) > 1 and "net buy/sell" in clean_text(row[1]).lower()
        ),
        None,
    )

    if header_index is None:
        raise WorkbookLayoutError(f"no sector header on the {sheet.title} sheet")

    sectors = {}

    for column, cell in enumerate(rows[header_index]):
        sgx_sector = clean_text(cell)

        if column < 2 or not sgx_sector:
            continue

        if sgx_sector not in SECTORS:
            # A new SGX sector needs a line in SECTORS, the rest of the sheet still loads
            LOGGER.warning(
                "[Parse] %s sheet: no mapping for SGX sector %r, skipped", 
                sheet.title, sgx_sector
            )
            continue

        sectors[column] = sgx_sector

    week_rows = []

    # A week row has the overall figure in column A, the footnotes below leave
    # it empty. Column B is usually a date, once text ("11 to 14 Dec-18")
    for row in rows[header_index + 1 :]:
        if len(row) < 2 or not isinstance(row[0], (int, float)) or row[1] is None:
            break

        week_rows.append(row)

    if not week_rows:
        raise WorkbookLayoutError(f"no weeks under the {sheet.title} sector header")

    weeks = [
        week - timedelta(weeks=len(week_rows) - 1 - index) 
        for index in range(len(week_rows))
    ]
    
    printed = [
        monday(row[1].date()) 
        if isinstance(row[1], datetime) else row_week
        for row, row_week in zip(week_rows, weeks)
    ]

    if printed != weeks:
        LOGGER.warning(
            "[Parse] %s sheet prints weeks %s, dated %s to %s from the workbook's week",
            sheet.title,
            ", ".join(day.isoformat() for day in printed),
            weeks[0],
            weeks[-1],
        )

    table = []

    for row, row_week in zip(week_rows, weeks):
        values = [(OVERALL, OVERALL, row[0])] + [
            (SECTORS[sgx_sector], sgx_sector, row[column] if column < len(row) else None)
            for column, sgx_sector in sectors.items()
        ]

        for sector, sgx_sector, value in values:
            if to_sgd(value) is None:
                continue

            table.append(
                {
                    "date": row_week.isoformat(),
                    "investor_type": investor_type,
                    "sector": sector,
                    "sgx_sector": sgx_sector,
                    "value": to_sgd(value),
                }
            )

    return table


def read_ranked_list(rows: list[list], start: int, column: int) -> list[tuple]:
    """
    (name, code, S$M) rows under a "Top 10 ..." header, from the given column
    """
    ranked = []

    for row in rows[start + 1 :]:
        name, code, value = (row[column : column + 3] + [None] * 3)[:3]

        if not name or not code or not isinstance(value, (int, float)):
            break

        ranked.append((clean_text(name), code, value))

    return ranked


def parse_top_10_sheet(sheet, week_start: str) -> list[dict]:
    rows = sheet_rows(sheet)
    table = []

    for index, row in enumerate(rows):
        header = clean_text(row[0] if row else None).lower()

        if not header.startswith("top 10"):
            continue

        investor_type = INSTITUTIONAL if "institution" in header else RETAIL

        # Net buys sit in columns A to C, net sells beside them in D to F
        for column, flow_type in ((0, NET_BUY), (3, NET_SELL)):
            for rank, (name, code, value) in enumerate(
                read_ranked_list(rows, index, column), start=1
            ):
                table.append(
                    {
                        "symbol": to_symbol(code),
                        "date": week_start,
                        "investor_type": investor_type,
                        "flow_type": flow_type,
                        "value": to_sgd(value),
                        "rank": rank,
                        "name": name,
                    }
                )

    if not table:
        raise WorkbookLayoutError("no Top 10 tables on the Weekly Top 10 sheet")

    warn_name_on_two_codes(table)

    return table


def warn_name_on_two_codes(table: list[dict]) -> None:
    """
    SGX has typed a company's name beside another stock's code, so the same
    name shows up under two codes in one week's lists. Only the code is
    stored, so the warning is a prompt to check that code against the file
    """
    codes_by_name = {}
    # Matched without case, logged as SGX printed it
    printed_name = {}

    for row in table:
        key = row["name"].lower()
        codes_by_name.setdefault(key, set()).add(row["symbol"])
        printed_name.setdefault(key, row["name"])

    for key, codes in codes_by_name.items():
        if len(codes) > 1:
            LOGGER.warning(
                "[Parse] week of %s: %r is printed beside %s, check which code it belongs to",
                table[0]["date"],
                printed_name[key],
                ", ".join(sorted(codes)),
            )


def parse_sti_sheet(sheet, week_start: str) -> list[dict]:
    rows = sheet_rows(sheet)

    header_index = next(
        (
            index
            for index, row in enumerate(rows)
            if row and clean_text(row[0]).lower().startswith("sti constituents")
        ),
        None,
    )

    if header_index is None:
        return []

    columns = {}

    for column, cell in enumerate(rows[header_index]):
        text = clean_text(cell).lower()

        if text.startswith("institution"):
            columns[INSTITUTIONAL] = column

        elif text.startswith("retail"):
            columns[RETAIL] = column

    table = []

    for row in rows[header_index + 1 :]:
        name, code = (row + [None, None])[:2]

        if not name or not code:
            break

        for investor_type, column in columns.items():
            value = row[column] if column < len(row) else None

            if isinstance(value, (int, float)):
                table.append(
                    {
                        "symbol": to_symbol(code),
                        "date": week_start,
                        "investor_type": investor_type,
                        # No list on this sheet, the sign gives the flow type
                        "flow_type": NET_BUY if value >= 0 else NET_SELL,
                        "value": to_sgd(value),
                        "rank": None,
                        "name": clean_text(name),
                    }
                )

    return table


def parse_workbook(content: bytes, title: str = "") -> dict:
    """
    The week the workbook covers, its sector rows (4 weeks a sheet) and its
    stock rows (that week only)
    """
    workbook = openpyxl.load_workbook(BytesIO(content), data_only=True, read_only=False)

    top_10 = find_sheet(workbook, TOP_10_SHEET)

    if top_10 is None:
        raise WorkbookLayoutError("no Weekly Top 10 sheet")

    week = parse_week(clean_text(top_10.cell(1, 1).value)) or parse_week(title)

    if week is None:
        raise WorkbookLayoutError("no 'Week of' date on the Weekly Top 10 sheet or title")

    week = monday(week)
    week_start = week.isoformat()

    sector_rows = []

    for sheet_name, investor_type in SECTOR_SHEETS.items():
        sheet = find_sheet(workbook, sheet_name)

        if sheet is None:
            raise WorkbookLayoutError(f"no {sheet_name} sheet")

        sector_rows += parse_sector_sheet(sheet, investor_type, week)

    stock_rows = parse_top_10_sheet(top_10, week_start)

    sti = find_sheet(workbook, STI_SHEET)

    if sti is not None:
        stock_rows += parse_sti_sheet(sti, week_start)

    return {
        "week_start": week_start,
        "sector_rows": sector_rows,
        "stock_rows": stock_rows,
    }
