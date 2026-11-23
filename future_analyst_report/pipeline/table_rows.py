from collections import defaultdict
from datetime import date

from periodic_financial.utils.currency import sgd_rate

import logging
import re


LOGGER = logging.getLogger(__name__)

TARGET_DECIMALS = 6

# SGX lists a house under more than one name, the key needs one per house
CONTRIBUTOR_NAMES = {"KGI Securities": "KGI Securities (Singapore)"}

# The house a title opens with, e.g. "CGSI - DFI Retail Group Update". SGX's
# contributor field is sometimes wrong (a CGSI note filed under Tickrs), the
# title is the house's own
TITLE_HOUSES = {
    "CGSI": "CGS International",
    "DBS": "DBS",
    "Maybank Securities": "Maybank Securities",
    "OCBC": "OCBC",
    "UOB KayHian": "UOB KayHian",
    "Phillip Capital": "POEMS by PhillipCapital",
    "KGI Securities": "KGI Securities (Singapore)",
}

# A house that rates a stock "Not Rated" says it does not cover it, so a
# target printed alongside (DBS's Equity Explorer) is never revised
NOT_RATED_PATTERN = re.compile(r"^\s*(not|non)[\s-]*rated|^\s*unrated", re.IGNORECASE)

# Each house names its levels differently, the page compares houses on one
# scale. DBS's Fully Valued is an expected loss of more than 10%
RATING_CLASSES = {
    "BUY": "buy",
    "ADD": "buy",
    "OUTPERFORM": "buy",
    "ACCUMULATE": "buy",
    "HOLD": "hold",
    "NEUTRAL": "hold",
    "SELL": "sell",
    "FULLY VALUED": "sell",
}

# SGX often lists one note twice a day or so apart, e.g. a first take and the
# full note, with the same call
DUPLICATE_NOTE_DAYS = 3

# SGX's copy of a report wins over the one on the house's own site
SGX_HOST = "api2.sgx.com"


def rating_class(rating: str | None) -> str | None:
    return RATING_CLASSES.get((rating or "").split("(")[0].strip().upper())


def group_by_house(rows: list[dict]) -> dict[tuple, list[dict]]:
    rows_by_house = defaultdict(list)

    for row in rows:
        if NOT_RATED_PATTERN.match(row["rating"] or ""):
            LOGGER.info(
                "[Table][%s %s %s] rated %r, left out",
                row["symbol"],
                row["contributor"],
                row["published_date"],
                row["rating"],
            )
            continue

        # The house is part of the table's key, a report without one can't join
        if not row["contributor"]:
            continue

        contributor = CONTRIBUTOR_NAMES.get(row["contributor"], row["contributor"])
        title_house = TITLE_HOUSES.get((row["title"] or "").split(" - ")[0].strip())

        if title_house and title_house != contributor:
            LOGGER.warning(
                "[Table][%s %s] filed under %s, the title names %s",
                row["symbol"],
                row["published_date"],
                contributor,
                title_house,
            )
            contributor = title_house

        rows_by_house[(row["symbol"], contributor)].append(
            {**row, "contributor": contributor}
        )

    return rows_by_house


def add_stored_history(
    rows_by_house: dict[tuple, list[dict]],
    stored_rows: list[dict],
    rates: dict | None,
) -> None:
    for stored in stored_rows:
        house_rows = rows_by_house.get((stored["symbol"], stored["contributor"]))

        if not house_rows:
            continue

        # The table keeps SGD only. A house prints one stock's target in one
        # currency, so the new report's currency and the stored date's rate
        # give back the printed figure. Read before a site copy is removed below
        fx_rate = sgd_rate(
            rates,
            house_rows[0]["original_currency"],
            stored["date"]
        )[0] or 1.0

        same_report = [
            row
            for row in house_rows
            if row["published_date"] == stored["date"]
        ]

        if same_report:
            # `source` is nullable, so a row written by a source without a URL
            # would raise a TypeError here and abort the whole CLEAN stage.
            stored_source = stored["source"] or ""
            new_source = same_report[0]["source_url"] or ""

            if SGX_HOST not in stored_source or SGX_HOST in new_source:
                # A re-extracted report replaces its stored row
                continue

            LOGGER.debug(
                "[Table][%s %s %s] SGX's copy is stored, the site's copy is left out",
                stored["symbol"],
                stored["contributor"],
                stored["date"],
            )
            house_rows.remove(same_report[0])

        house_rows.append(
            {
                "symbol": stored["symbol"],
                "contributor": stored["contributor"],
                "published_date": stored["date"],
                "analyst": stored["analyst"],
                "title": stored["title"],
                "source_url": stored["source"],
                "rating": stored["rating"],
                "original_target_price": round(stored["target_price"] / fx_rate, 4),
                "previous_target_price": stored["previous_target_price"],
                "fx_rate": fx_rate,
                "target_basis": stored["target_basis"],
                "forecasts": stored["forecast"] or [],
                "forecast_basis": {},
                "stored": True,
            }
        )


def merge_repeat_notes(house_rows: list[dict]) -> list[dict]:
    notes = []

    for row in sorted(house_rows, key=lambda house_row: house_row["published_date"]):
        earlier = notes[-1] if notes else None

        if (
            earlier
            and earlier["original_target_price"] == row["original_target_price"]
            and rating_class(earlier["rating"]) == rating_class(row["rating"])
            and (
                date.fromisoformat(row["published_date"])
                - date.fromisoformat(earlier["published_date"])
            ).days
            <= DUPLICATE_NOTE_DAYS
        ):
            # The call dates from the first filing, the forecast table
            # often only comes with the fuller second one
            LOGGER.info(
                "[Table][%s %s] %s repeats the %s note, merged",
                row["symbol"],
                row["contributor"],
                row["published_date"],
                earlier["published_date"],
            )

            if not earlier["forecasts"] and row["forecasts"]:
                # A stored first take is rewritten under its own date with
                # the fuller note's figures
                notes[-1] = (
                    {**row, "published_date": earlier["published_date"]}
                    if earlier.get("stored")
                    else {
                        **earlier,
                        "forecasts": row["forecasts"],
                        "forecast_basis": row["forecast_basis"],
                    }
                )

            continue

        notes.append(row)

    return notes


def previous_printed_targets(notes: list[dict]) -> list[float | None]:
    previous_targets = []

    for index, row in enumerate(notes):
        if row["previous_target_price"] is not None:
            previous_targets.append(row["previous_target_price"] / row["fx_rate"])

        elif index == 0:
            previous_targets.append(None)

        else:
            # Compared as printed, since an unchanged US$ target converts to
            # a new SGD figure each quarter. A maintained target carries the
            # one before the house's last change
            earlier_target = notes[index - 1]["original_target_price"]
            previous_targets.append(
                earlier_target
                if earlier_target != row["original_target_price"]
                else previous_targets[-1]
            )

    return previous_targets


def clean_forecast(row: dict, target_price: float) -> list[dict]:
    basis = row.get("forecast_basis") or {}
    forecast = []

    for year in row["forecasts"]:
        per_share = {}

        # An EPS above the house's own target is a P/E below 1, and a DPS
        # above it a yield over 100%, so either is a misprint in the report
        for field in ("eps", "dps"):
            value = year[field]

            if value is not None and value > target_price * 100:
                LOGGER.warning(
                    "[Table][%s %s %s] FY%s %s %s cents is above the S$%s target, dropped",
                    row["symbol"],
                    row["contributor"],
                    row["published_date"],
                    year["year"],
                    field,
                    value,
                    target_price,
                )
                value = None

            per_share[field] = value

        forecast.append(
            {
                "year": year["year"],
                "label": year.get("label"),
                "period_end": year.get("period_end"),
                "revenue": year["revenue"],
                "net_profit": year["net_profit"],
                "profit_type": basis.get("profit_type") or "reported",
                "eps": per_share["eps"],
                "dps": per_share["dps"],
                "dividend_yield": year["dividend_yield"],
                "pe": year["pe"],
                "pb": year["pb"],
            }
        )

    return forecast


def to_table_row(row: dict, previous_printed: float | None) -> dict:
    rating = rating_class(row["rating"])

    if row["rating"] and not rating:
        LOGGER.warning(
            "[Table][%s %s %s] rating %r has no class, left empty",
            row["symbol"],
            row["contributor"],
            row["published_date"],
            row["rating"],
        )

    target_price = round(row["original_target_price"] * row["fx_rate"], TARGET_DECIMALS)

    return {
        "symbol": row["symbol"],
        "contributor": row["contributor"],
        "date": row["published_date"],
        "analyst": row["analyst"],
        "title": row["title"],
        "source": row["source_url"],
        "rating": rating,
        "target_price": target_price,
        "previous_target_price": (
            None
            if previous_printed is None
            else round(previous_printed * row["fx_rate"], TARGET_DECIMALS)
        ),
        "target_basis": row["target_basis"],
        "forecast": clean_forecast(row, target_price) or None,
    }


def build_table_rows(
    rows: list[dict],
    stored_rows: list[dict] | None = None,
    rates: dict | None = None,
) -> list[dict]:
    """
    Table rows for the new reports. stored_rows are the house's reports already
    in sgx_analyst_report: they settle the previous target and catch a repeat
    filing, but are never written again
    """
    rows_by_house = group_by_house(rows)
    add_stored_history(rows_by_house, stored_rows or [], rates)

    table_rows = []

    for house_rows in rows_by_house.values():
        notes = merge_repeat_notes(house_rows)

        for row, previous_printed in zip(notes, previous_printed_targets(notes)):
            if not row.get("stored"):
                table_rows.append(to_table_row(row, previous_printed))

    return sorted(
        table_rows,
        key=lambda table_row: (table_row["symbol"], table_row["date"])
    )
