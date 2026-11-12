from pathlib import Path

from .api import (
    iter_announcements,
    get_sub_title,
    is_month_end,
    fetch_announcement_detail,
    download_document,
)
from .document_check import (
    get_name_rank,
    is_pdf,
    check_document,
    check_scanned_document,
)
from ..pipeline.parse_pdf import PDF_CACHE_DIR, get_cache_path
from ..utils.io_helper import open_json, write_json
from ..llm.usage import track_usage, summarise_usage

from calendar import monthrange
from datetime import date, datetime, timezone

import logging


LOGGER = logging.getLogger(__name__)


# Sub titles that carry statements, mapped to the half they are stored as
PERIOD_BY_SUB_TITLE = {
    "Full Yearly Results": "H2",
    "Half Yearly Results": "H1",
    "Second Quarter and/ or Half Yearly Results": "H1",
}

SYMBOL_SUFFIX = ".SI"

ISSUER_CODES_PATH = Path(__file__).parent / "issuer_codes.json"

DEFAULT_TOP = 200

# Results are due within months of the period end, a full year later is a typo
MAX_FILING_DELAY_DAYS = 300
DEFAULT_DAYS = 7


def normalise_issuer_name(name: str | None) -> str:
    return " ".join((name or "").casefold().split())


def update_issuer_codes(announcements: list[dict]) -> dict[str, list[str]]:
    # Some announcements come back with issuers null and only the issuer name.
    # The name is mapped to its stock codes from the announcements that do list
    # them, saved so a short daily window still knows a company that filed
    # nothing else that week
    codes_by_name = open_json(ISSUER_CODES_PATH) if ISSUER_CODES_PATH.exists() else {}

    for announcement in announcements:
        for issuer in announcement.get("issuers") or []:
            name = normalise_issuer_name(issuer.get("issuer_name"))
            code = issuer.get("stock_code")

            if name and code and code not in codes_by_name.setdefault(name, []):
                codes_by_name[name].append(code)

    write_json(ISSUER_CODES_PATH, codes_by_name)

    return codes_by_name


def get_symbols(
    announcement: dict,
    top_symbols: set[str],
    codes_by_name: dict[str, list[str]],
) -> list[str]:
    # One company can trade on two counters and every results announcement
    # lists both. Each counter is stored, so the filing goes to every listed
    # issuer, not just the first
    codes = [issuer.get("stock_code") for issuer in announcement.get("issuers") or []]

    # A company that changed its stock code filed its earlier results under
    # the old one, so the issuer name also brings in every code the company
    # is known by. A company can also change its name, so a code seen under
    # another name brings in that name's codes too
    codes += codes_by_name.get(
        normalise_issuer_name(announcement.get("issuer_name")), []
    )
    codes += [
        alias
        for name_codes in codes_by_name.values()
        if set(codes) & set(name_codes)
        for alias in name_codes
    ]

    symbols = []

    for code in codes:
        symbol = f"{code}{SYMBOL_SUFFIX}"

        if symbol in top_symbols and symbol not in symbols:
            symbols.append(symbol)

    return symbols


def correct_typed_year(period_ended: str, broadcast: str) -> str:
    # Results come out within months of the period end. An announcement can
    # type last year's date, which groups it with last year's real filing.
    # More than a year after the typed date means the period is a year later.
    # The statements check confirms the date afterwards
    period_end = date.fromisoformat(period_ended)

    if (date.fromisoformat(broadcast[:10]) - period_end).days <= MAX_FILING_DELAY_DAYS:
        return period_ended

    LOGGER.warning(
        "Period ended %s typed on an announcement broadcast %s, using %s",
        period_ended,
        broadcast[:10],
        add_one_year(period_ended),
    )

    return add_one_year(period_ended)


def get_broadcast(announcement: dict) -> str:
    broadcast_ms = announcement.get("broadcast_date_time") or 0
    return datetime.fromtimestamp(broadcast_ms / 1000, tz=timezone.utc).isoformat()


def find_filings(
    period_start: date,
    period_end: date,
    top_symbols: set[str],
) -> dict[tuple[str, str], list[dict]]:
    filings = {}
    announcements = list(iter_announcements(period_start, period_end))
    codes_by_name = update_issuer_codes(announcements)

    for announcement in announcements:
        sub_title = get_sub_title(announcement)
        symbols = get_symbols(announcement, top_symbols, codes_by_name)

        if sub_title not in PERIOD_BY_SUB_TITLE or not symbols:
            continue

        symbol = symbols[0]

        detail = fetch_announcement_detail(announcement["url"])
        period_ended = detail.get("period_ended")

        LOGGER.info(
            "[Announcements][%s] %s %s, period ended %s, %s",
            symbol,
            announcement["ref_id"],
            detail.get("status"),
            period_ended,
            announcement["url"],
        )

        if period_ended is None:
            LOGGER.warning(
                "[Announcements][%s] %s: no period ended on the announcement, skipped",
                symbol,
                announcement["ref_id"],
            )
            continue

        broadcast = get_broadcast(announcement)
        period_ended = correct_typed_year(period_ended, broadcast)

        # A replacement keeps the original's ref_id, and some issuers file the
        # statements and the results under several announcements, so every
        # version is grouped by symbol and period month, one half can be dated
        # 29/09/2023 on some announcements and 30/09/2023 on others
        for symbol in symbols:
            filings.setdefault((symbol, period_ended[:7]), []).append(
                {
                    "reference": announcement["ref_id"],
                    "status": detail.get("status"),
                    "sub_title": sub_title,
                    "period_ended": period_ended,
                    "broadcast": broadcast,
                    "announcement_url": announcement["url"],
                    "description": detail.get("description"),
                    "attachments": detail["attachments"],
                }
            )

    return {
        (symbol, get_period_end(versions)): versions
        for (symbol, _), versions in filings.items()
    }


def get_period_end(versions: list[dict]) -> str:
    # A month end wins when any version gives one, a 52-week year closing on
    # a weekday keeps its own date
    period_ends = [version["period_ended"] for version in versions]
    month_ends = [
        period_end
        for period_end in period_ends
        if is_month_end(date.fromisoformat(period_end))
    ]

    return max(month_ends or period_ends)


def get_candidates(versions: list[dict]) -> list[dict]:
    # A replacement can carry only the file it adds (slides, a corrigendum),
    # so the statements stay on the original: attachments are
    # pooled across versions, and a re-uploaded file keeps its newest copy
    newest_first = sorted(
        versions, key=lambda version: version["broadcast"], reverse=True
    )
    candidates = {}

    for version in newest_first:
        for attachment in version["attachments"]:
            if not is_pdf(attachment["name"], attachment["url"]):
                continue

            candidates.setdefault(
                attachment["name"].casefold(),
                {
                    **attachment,
                    "reference": version["reference"],
                    "version_status": version["status"],
                    "broadcast": version["broadcast"],
                    "name_rank": get_name_rank(attachment["name"], attachment["url"]),
                },
            )

    return list(candidates.values())


def sort_candidates(candidates: list[dict]) -> list[dict]:
    # Statement-like names are opened first, the newest version first within a rank
    by_newest = sorted(
        candidates, key=lambda candidate: candidate["broadcast"], reverse=True
    )

    return sorted(by_newest, key=lambda candidate: candidate["name_rank"])


def is_accepted(check: dict) -> bool:
    return bool(check.get("is_statements") and check.get("is_period"))


def add_one_year(period_end: str) -> str:
    parsed = date.fromisoformat(period_end)

    if is_month_end(parsed):
        return date(
            parsed.year + 1, parsed.month, monthrange(parsed.year + 1, parsed.month)[1]
        ).isoformat()

    return parsed.replace(year=parsed.year + 1).isoformat()


def correct_period_end(pdf_bytes: bytes, period_end: str, versions: list[dict]) -> str:
    # An announcement can type last year's period end. The statements print
    # last year's date as the comparative, so they pass for the
    # typed year too. When a year later has also passed by the filing date and
    # the statements print it, that later date is the period they report
    next_period_end = add_one_year(period_end)
    first_broadcast = min(version["broadcast"] for version in versions)[:10]

    if next_period_end >= first_broadcast:
        return period_end

    if not is_accepted(check_document(pdf_bytes, next_period_end)):
        return period_end

    LOGGER.warning(
        "Announcement says period ended %s, the statements report %s, using %s",
        period_end,
        next_period_end,
        next_period_end,
    )

    return next_period_end


def build_selection(
    symbol: str,
    period_end: str,
    candidate: dict,
    pdf_bytes: bytes,
    checked: list[dict],
    use_cache: bool,
) -> dict:
    if use_cache:
        # Saved under parse_pdf's own cache name, so step 1 does not download it again
        PDF_CACHE_DIR.mkdir(exist_ok=True)
        get_cache_path(symbol, period_end, candidate["url"]).write_bytes(pdf_bytes)

    return {
        "status": "selected",
        "period_end": period_end,
        "document": candidate,
        "checked": checked,
    }


def select_document(
    symbol: str,
    period_end: str,
    versions: list[dict],
    use_cache: bool = True,
) -> dict:
    candidates = sort_candidates(get_candidates(versions))
    checked = []
    scanned_candidates = []

    for candidate in candidates:
        pdf_bytes = download_document(candidate["url"])

        if pdf_bytes is None:
            checked.append({"name": candidate["name"], "result": "download_failed"})
            continue

        check = check_document(pdf_bytes, period_end)
        checked.append({"name": candidate["name"], "checked_by": "text", **check})

        # Both have to hold: the statements as tables, and the period we file under.
        # A deck or press release fails the first, last year's statements the second
        if is_accepted(check):
            reported_period_end = correct_period_end(pdf_bytes, period_end, versions)
            return build_selection(
                symbol, reported_period_end, candidate, pdf_bytes, checked, use_cache
            )

        if check.get("is_scanned"):
            scanned_candidates.append((candidate, pdf_bytes, check["page_count"]))

    # Only when nothing passed on text: a scan has no text to read, so Haiku
    # looks at its pages instead. Most filings never reach this tho
    for candidate, pdf_bytes, page_count in scanned_candidates:
        check = check_scanned_document(
            pdf_bytes,
            period_end,
            log_name=f"{symbol} {period_end} {candidate['name']}",
            page_count=page_count,
        )
        checked.append({"name": candidate["name"], **check})

        if is_accepted(check):
            return build_selection(
                symbol, period_end, candidate, pdf_bytes, checked, use_cache
            )

    return {
        "status": "no_statements_document",
        "document": None,
        "checked": checked,
    }


def build_result(
    symbol: str,
    period_end: str,
    versions: list[dict],
    selection: dict,
) -> dict:
    newest = max(versions, key=lambda version: version["broadcast"])
    document = selection["document"] or {}

    return {
        "symbol": symbol,
        # The period the statements report, which can differ from a typo on the announcement
        "date": selection.get("period_end") or period_end,
        "announced_period_end": period_end,
        "period": PERIOD_BY_SUB_TITLE[newest["sub_title"]],
        "source_url": document.get("url"),
        "status": selection["status"],
        "document_name": document.get("name"),
        "reference": document.get("reference") or newest["reference"],
        "versions": [
            {
                "reference": version["reference"],
                "status": version["status"],
                "broadcast": version["broadcast"],
                "announcement_url": version["announcement_url"],
                "attachments": [
                    attachment["name"] for attachment in version["attachments"]
                ],
            }
            for version in versions
        ],
        "checked": selection["checked"],
    }


def fetch_document(symbol: str, period_end: str, versions: list[dict]) -> dict | None:
    # One filing's statements document, None when no attachment holds them
    log_name = f"{symbol} {period_end}"

    with track_usage() as usage_records:
        selection = select_document(symbol, period_end, versions)

    filing = build_result(symbol, period_end, versions, selection)
    filing["llm_usage"] = summarise_usage(usage_records)

    for version in filing["versions"]:
        LOGGER.info(
            "[Fetch][%s] announcement %s", log_name, version["announcement_url"]
        )

    if filing["status"] != "selected":
        LOGGER.warning(
            "[Fetch][%s] %s among %d attachment(s), skipped",
            log_name,
            filing["status"],
            len(filing["checked"]),
        )
        return None

    # The statements can report a later period than the announcement typed
    if filing["date"] != period_end:
        LOGGER.warning(
            "[Fetch][%s] statements report %s, filed under it", log_name, filing["date"]
        )

    LOGGER.info(
        "[Fetch][%s %s] document %s, %s",
        symbol,
        filing["date"],
        filing["document_name"],
        filing["source_url"],
    )

    return filing
