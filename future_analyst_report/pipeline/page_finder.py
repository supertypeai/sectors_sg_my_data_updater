import logging
import re


LOGGER = logging.getLogger(__name__)

# The rating, target price and key-financials sidebar sit on these
FRONT_PAGE_COUNT = 2

# Past this a report is a sector or strategy report covering many stocks
SHORT_REPORT_PAGES = 12

MAX_STOCK_PAGES = 6

# A report printed as images has no text to search, DBS's forecast table
# is often on page 3
IMAGE_ONLY_PAGES = 4

# "FY26F", "2027F", "27F", "FY26e", "Dec-26F", and "FY25A" beside them
FORECAST_YEAR_PATTERN = re.compile(
    r"(?<![\w/])"
    r"(?:FY|CY|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\s']?)?"
    r"(?:20)?(\d{2})[EFA](?![\w])",
    re.IGNORECASE,
)

FORECAST_ROW_PATTERN = re.compile(
    r"\b(revenue|turnover|total\s+(?:operating\s+)?income|net\s+interest\s+income"
    r"|gross\s+revenue|net\s+property\s+income|NPI|core\s+net\s+profit|net\s+profit"
    r"|PATMI|net\s+income|distributable\s+income|EPS|DPS|DPU|earnings\s+per\s+share"
    r"|dividends?\s+per\s+share|distributions?\s+per\s+unit|P/E|PER|P/B|P/BV|PBV"
    r"|dividend\s+yield|distribution\s+yield)\b"
    r"([^\n]*)",
    re.IGNORECASE,
)

FIGURE_PATTERN = re.compile(r"\(?-?\d[\d,]*\.?\d*\)?%?")

MIN_FORECAST_YEARS = 2
MIN_FORECAST_ROWS = 2
MIN_ROW_FIGURES = 2

# A figure must follow, since the rating definitions on the disclosure pages
# say "target price" with no price
TARGET_WORDS = r"(?:(?i:target\s+price|price\s+target|fair\s+value)|\bTP\b)"
TARGET_PATTERN = re.compile(rf"{TARGET_WORDS}[^\n\d]{{0,30}}\d")
TARGET_HEADER_PATTERN = re.compile(TARGET_WORDS)

# One row per stock: price, target, then two years of each ratio
MIN_COVERAGE_ROW_FIGURES = 6

NAME_SUFFIX_PATTERN = re.compile(
    r"\b(?:ltd|limited|holdings?|group|corp|corporation|inc|plc|bhd|berhad|co)\b\.?",
    re.IGNORECASE,
)

INITIALISM_SKIP_WORDS = {"ltd", "limited", "inc", "plc", "bhd", "berhad", "co"}

MIN_INITIALISM_LETTERS = 3

# Names reports use that neither the listed name nor its initials give,
# mostly the short codes in brokers' REIT coverage tables
STOCK_ALIASES = {
    "Z74": ["Singtel"],
    "S68": ["SGX"],
    "Y92": ["ThaiBev"],
    "9CI": ["CLI"],
    "S63": ["ST Engineering", "STE"],
    "CJLU": ["NetLink"],
    "A7RU": ["KIT"],
    "8C8U": ["CAREIT", "Centurion REIT"],
    "A17U": ["CLAR"],
    "AJBU": ["KDCREIT", "Keppel DC"],
    "M44U": ["MLT"],
    "ME8U": ["MINT"],
    "J69U": ["FCT"],
    "K71U": ["KREIT"],
    "T82U": ["SUN", "Suntec REIT"],
    "HMN": ["CLAS"],
    "C2PU": ["PREIT", "PLife REIT"],
    "TS0U": ["OUECT", "OUEREIT"],
    "JYEU": ["LREIT"],
    "9A4U": ["EREIT", "ESR REIT"],
    "CY6U": ["CLINT"],
    "P40U": ["SGREIT"],
    "O5RU": ["AAREIT"],
    "AU8U": ["CLCT"],
    "J85": ["CDLHT", "CDL Hospitality"],
    "DCRU": ["DCREIT"],
    "ODBU": ["UHREIT"],
    "CMOU": ["KORE"],
    "UIBU": ["UIBREIT"],
    "DHLU": ["DHLT"],
}


def build_stock_patterns(symbol: str, company_name: str | None) -> list[re.Pattern]:
    security_code = symbol.removesuffix(".SI")
    patterns = [re.compile(rf"\b{re.escape(security_code)}\b")]
    aliases = list(STOCK_ALIASES.get(security_code, []))

    if company_name:
        # "China Aviation Oil (Singapore) Corp Ltd" is written without the brackets
        company_name = re.sub(r"\([^)]*\)", " ", company_name)
        short_name = re.sub(r"\s+", " ", NAME_SUFFIX_PATTERN.sub("", company_name))
        short_name = short_name.strip(" ,.")
        aliases.append(short_name)
        aliases.append(
            re.sub(r"real estate investment trust", "REIT", short_name, flags=re.IGNORECASE)
        )

        # "Oversea-Chinese Banking Corp Ltd" reads "OCBC", matched case-sensitive
        initialism = "".join(
            word[0]
            for word in re.split(r"[\s\-]+", company_name)
            if word
            and word[0].isalpha()
            and word.lower().strip(".") not in INITIALISM_SKIP_WORDS
        ).upper()

        if len(initialism) >= MIN_INITIALISM_LETTERS:
            patterns.append(re.compile(rf"\b{initialism}\b"))

    # A code in capitals matches only in capitals, so "SUN" skips "sun"
    for alias in aliases:
        if alias:
            flags = 0 if alias.isupper() else re.IGNORECASE
            patterns.append(re.compile(rf"\b{re.escape(alias)}\b", flags))

    return patterns


def mentions_stock(text: str, stock_patterns: list[re.Pattern]) -> bool:
    return any(pattern.search(text) for pattern in stock_patterns)


def has_forecast_years(page_text: str) -> bool:
    return len(set(FORECAST_YEAR_PATTERN.findall(page_text))) >= MIN_FORECAST_YEARS


def summarise_pages(page_numbers: list[int], image_pages: set[int]) -> str:
    return (
        ",".join(
            f"{page_number}{'?' if page_number in image_pages else ''}"
            for page_number in page_numbers
        )
        or "-"
    )


def find_pages(parser_result: dict, symbol: str, company_name: str | None) -> dict:
    pages = parser_result["pages"]
    page_count = parser_result["page_count"]
    image_pages = set(parser_result["image_pages"])
    stock_patterns = build_stock_patterns(symbol, company_name)

    front_pages = [page["page_number"] for page in pages[:FRONT_PAGE_COUNT]]
    stock_pages, forecast_pages, target_pages = [], [], []

    # Only pages naming the stock, so a sector report gives its table, not a peer's
    for page in pages:
        page_text = page["text"]

        if not mentions_stock(page_text, stock_patterns):
            continue

        stock_pages.append(page["page_number"])
        forecast_years = has_forecast_years(page_text)

        # Anywhere on the line: reading order puts a sidebar beside body text
        row_names = {
            re.sub(r"\s+", " ", row_name.lower())
            for row_name, rest_of_line in FORECAST_ROW_PATTERN.findall(page_text)
            if len(FIGURE_PATTERN.findall(rest_of_line)) >= MIN_ROW_FIGURES
        }

        if forecast_years and len(row_names) >= MIN_FORECAST_ROWS:
            forecast_pages.append(page["page_number"])

        is_coverage_table = (
            forecast_years
            and TARGET_HEADER_PATTERN.search(page_text)
            and any(
                mentions_stock(line, stock_patterns)
                and len(FIGURE_PATTERN.findall(line)) >= MIN_COVERAGE_ROW_FIGURES
                for line in page_text.splitlines()
            )
        )

        if TARGET_PATTERN.search(page_text) or is_coverage_table:
            target_pages.append(page["page_number"])

    if len(image_pages) == page_count:
        selection = "image_only"
        page_numbers = [page["page_number"] for page in pages[:IMAGE_ONLY_PAGES]]

    elif not stock_pages:
        selection = "front_only"
        page_numbers = front_pages

    # Forecast years but no table matched: cheaper to send a short report
    # whole than to store an empty forecast
    elif (
        page_count <= SHORT_REPORT_PAGES
        and not forecast_pages
        and any(has_forecast_years(page["text"]) for page in pages)
    ):
        selection = "whole_report"
        page_numbers = [page["page_number"] for page in pages]

    else:
        selection = "front_and_forecast" if forecast_pages else "front_and_target"
        later_pages = [
            page_number
            for page_number in dict.fromkeys(forecast_pages + target_pages)
            if page_number not in front_pages
        ]
        page_numbers = sorted(front_pages + later_pages[:MAX_STOCK_PAGES])

    record = {
        "symbol": symbol,
        "document_url": parser_result["document_url"],
        "page_count": page_count,
        "selection": selection,
        "page_numbers": page_numbers,
        "forecast_pages": forecast_pages,
        "target_pages": target_pages,
        "image_pages": [
            page_number for page_number in page_numbers if page_number in image_pages
        ],
    }

    log_name = f"{symbol} {parser_result['contributor']} {parser_result['report_date']}"

    # Usually a sector note that only lists the stock, the extract line says
    # whether a target came out of it
    if selection == "front_only":
        LOGGER.debug("[Page Finder][%s] stock not named in the report", log_name)

    LOGGER.debug(
        "[Page Finder][%s] %s of %d | pages %s | forecast %s | target %s",
        log_name,
        selection,
        page_count,
        summarise_pages(page_numbers, image_pages),
        summarise_pages(forecast_pages, image_pages),
        summarise_pages(target_pages, image_pages),
    )

    return record
