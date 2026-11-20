from ..pipeline.page_finder import build_stock_patterns, mentions_stock

import re


def build_company_patterns(companies: list[dict]) -> list[tuple[str, list[re.Pattern]]]:
    return [
        (company["symbol"], build_stock_patterns(company["symbol"], company["name"]))
        for company in companies
    ]


def match_symbols(
    title: str, company_patterns: list[tuple[str, list[re.Pattern]]]
) -> list[str]:
    # External sites don't tag SGX codes, so a report belongs to every
    # company its title names, the extraction then confirms a target
    return [
        symbol
        for symbol, stock_patterns in company_patterns
        if mentions_stock(title, stock_patterns)
    ]
