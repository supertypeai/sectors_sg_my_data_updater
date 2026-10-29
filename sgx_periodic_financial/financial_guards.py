"""
Accounting sanity checks for extracted SGX financials.

The extractor reads figures out of a PDF with a language model, so its failures
are not random noise: they are misread columns, a value picked from the wrong
line, a scale misread ("S$'000" treated as units) or one field contaminated by
another. All of those break arithmetic that real financial statements satisfy,
which makes them cheap to catch without a second model call.

Two kinds of check:

  * IDENTITIES — relationships that must hold in any statement
    (assets = liabilities + equity, pretax - tax = net income after tax, ...). A break
    means at least one of the components is wrong, but not which, so these are
    reported and the row is flagged rather than silently edited.

  * IMPOSSIBILITIES — a single value that cannot be what it claims to be
    (revenue equal to total assets, revenue below net income, negative assets,
    a figure larger than any SGX issuer). These identify the offending field, so
    that field is nulled: a missing number is recoverable, a plausible-looking
    wrong one is not.

`check()` returns the issues; `apply()` returns cleaned metrics plus the issue
list, and is what the pipeline calls before writing a row.
"""

import math
import statistics

# Identity checks allow for rounding in the source document and for the
# extractor picking up a figure printed to a different precision.
IDENTITY_TOLERANCE = 0.02
# Below this, a percentage difference is meaningless (rounding on small numbers).
ABS_FLOOR = 1_000.0
# No SGX issuer has a statement line this large; anything above is a scale error.
# DBS, the largest, holds ~S$850bn of assets, so 2tn leaves generous headroom
# while still catching a "S$'000 read as units" misread (BWM's 4.4tn balance
# sheet was really 4.4bn).
IMPLAUSIBLE = 2e12
# Share counts are the one line that legitimately runs into the billions;
# no SGX issuer has 100bn shares in issue.
IMPLAUSIBLE_SHARES = 1e11

# Pairs that legitimately hold the same figure: `ebit` is deliberately mirrored
# onto `operating_income`, and basic/diluted share counts match when there is no
# dilution. Every other exact repeat of a large value across two fields means one
# number was read once and written twice.
ALLOWED_EQUAL_PAIRS = {
    frozenset({"ebit", "operating_income"}),
    frozenset({"basic_shares_outstanding", "diluted_shares_outstanding"}),
    frozenset({"total_common_shares", "diluted_shares_outstanding"}),
    frozenset({"total_revenue", "gross_income"}),
}
# Small values repeat by coincidence (zeros, small counts); large ones do not.
DUPLICATE_FLOOR = 1_000_000

# Line items that are never one of these metrics. The extractor once read DBS's
# "commitments that are unconditionally cancellable ... $363,808 million" -- an
# off-balance-sheet footnote -- as both total revenue and total assets, with high
# self-reported confidence, because it had read the number correctly and only
# chosen the wrong line.
FORBIDDEN_SOURCE_TERMS = (
    "commitment", "contingent", "notional", "off-balance", "off balance",
    "guarantee", "unutilised", "undrawn", "collateral", "nominal value",
)

IS = "income_statement"
BS = "balance_sheet"
CF = "cash_flow"


# Cash flow is an equation, not a ratio: a residual here is rounding in the
# source statement, so it must be small in absolute terms or the row is broken.
# 2,397 of 2,961 stored rows reconcile to the dollar and 487 more within S$1,000.
CASH_FLOW_TOLERANCE = 1_000.0


def _get(metrics: dict, group: str, field: str):
    value = (metrics.get(group) or {}).get(field)
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _close(left: float, right: float, tolerance: float = IDENTITY_TOLERANCE) -> bool:
    """Relative comparison that degrades to absolute for small magnitudes."""
    scale = max(abs(left), abs(right))
    if scale < ABS_FLOOR:
        return abs(left - right) < ABS_FLOOR
    return abs(left - right) / scale <= tolerance


def _cf_close(left: float, right: float) -> bool:
    return abs(left - right) <= CASH_FLOW_TOLERANCE


def _identity(metrics, issues, group, target, parts, label):
    """`target` should equal the sum of `parts`, when all are present."""
    total = _get(metrics, group, target)
    values = [_get(metrics, group, p) for p in parts]
    if total is None or any(v is None for v in values):
        return
    expected = sum(values)
    if not _close(total, expected):
        issues.append({
            "check": label,
            "severity": "identity",
            "fields": [target, *parts],
            "detail": f"{target}={total:,.0f} vs {'+'.join(parts)}={expected:,.0f}"
                      f" ({_ratio(total, expected)})",
        })


def _ratio(left: float, right: float) -> str:
    if right == 0:
        return "n/a"
    return f"{left / right:.2f}x"


def check(metrics: dict) -> list[dict]:
    """Every rule that fails on this row, worst first."""
    issues: list[dict] = []

    # --- identities -------------------------------------------------------
    # The balance sheet must balance; this is the single strongest signal that
    # something in the row was misread.
    assets = _get(metrics, BS, "total_asset")
    liabilities = _get(metrics, BS, "total_liabilities")
    equity = _get(metrics, BS, "total_equity")
    if assets is not None and liabilities is not None and equity is not None:
        if not _close(assets, liabilities + equity):
            issues.append({
                "check": "balance_sheet_balances",
                "severity": "identity",
                "fields": ["total_asset", "total_liabilities", "total_equity"],
                "detail": f"assets={assets:,.0f} vs liabilities+equity="
                          f"{liabilities + equity:,.0f} ({_ratio(assets, liabilities + equity)})",
            })

    _identity(metrics, issues, BS, "total_asset",
              ["total_current_asset", "total_non_current_asset"], "assets_split")
    _identity(metrics, issues, BS, "total_liabilities",
              ["total_current_liabilities", "total_non_current_liabilities"],
              "liabilities_split")

    # Cash flow sum check: operating + investing + financing. The provider is not
    # uniform about the FX line: for some issuers netChangeInCash already includes
    # it (net equals the activities), for others the effect is reported separately
    # and subtracted (net equals activities - fx). `activities + fx` matches no row
    # in the corpus, so it is not offered as a candidate. The residual is an
    # absolute amount, not a ratio: source rounding is at most a few thousand.
    net_cf = _get(metrics, CF, "net_cash_flow")
    ocf = _get(metrics, CF, "operating_cash_flow")
    icf = _get(metrics, CF, "investing_cash_flow")
    fcf = _get(metrics, CF, "financing_cash_flow")
    forex_cf = _get(metrics, CF, "effect_of_exchange_rate_changes")
    if forex_cf is None:
        forex_cf = _get(metrics, CF, "forex_effect_on_cash")
    if None not in (net_cf, ocf, icf, fcf):
        activities = ocf + icf + fcf
        candidates = [activities]
        if forex_cf:
            candidates.append(activities - forex_cf)
        if not any(_cf_close(net_cf, candidate) for candidate in candidates):
            issues.append({
                "check": "cash_flow_sums",
                "severity": "identity" if forex_cf else "review",
                "fields": ["net_cash_flow", "operating_cash_flow", "investing_cash_flow", "financing_cash_flow"],
                "detail": f"net_cash_flow={net_cf:,.0f} vs sum={activities:,.0f}"
                          + (f" (fx={forex_cf:,.0f})" if forex_cf else ""),
            })

    # working_capital = current assets - current liabilities
    current_assets = _get(metrics, BS, "total_current_asset")
    current_liabilities = _get(metrics, BS, "total_current_liabilities")
    working_capital = _get(metrics, BS, "working_capital")
    if None not in (current_assets, current_liabilities, working_capital):
        if not _close(working_capital, current_assets - current_liabilities):
            issues.append({
                "check": "working_capital",
                "severity": "identity",
                "fields": ["working_capital"],
                "detail": f"{working_capital:,.0f} vs current assets-liabilities="
                          f"{current_assets - current_liabilities:,.0f}",
            })

    # pretax - tax = net income after tax. Some feeds store net income
    # attributable to owners, which is lower by minorities/extra items; those
    # rows carry net_income_after_taxes (the field the identity actually applies
    # to), so compare against that when present.
    pretax = _get(metrics, IS, "pretax_income")
    tax = _get(metrics, IS, "income_taxes")
    net_income = _get(metrics, IS, "net_income")
    net_after_tax = _get(metrics, IS, "net_income_after_taxes")
    bridge_net = net_after_tax if net_after_tax is not None else net_income
    if None not in (pretax, tax, bridge_net) and not _close(bridge_net, pretax - tax):
        issues.append({
            "check": "tax_bridge",
            "severity": "identity",
            "fields": ["pretax_income", "income_taxes",
                       "net_income_after_taxes" if net_after_tax is not None else "net_income"],
            "detail": f"net={bridge_net:,.0f} vs pretax-tax={pretax - tax:,.0f}",
        })

    # gross = revenue - cost of revenue
    revenue = _get(metrics, IS, "total_revenue")
    cost = _get(metrics, IS, "cost_of_revenue")
    gross = _get(metrics, IS, "gross_income")
    if None not in (revenue, cost, gross) and not _close(gross, revenue - cost):
        issues.append({
            "check": "gross_bridge",
            "severity": "identity",
            "fields": ["gross_income", "total_revenue", "cost_of_revenue"],
            "detail": f"gross={gross:,.0f} vs revenue-cost={revenue - cost:,.0f}",
        })

    # --- impossibilities: these name a specific bad field ------------------
    # Revenue equal to a balance sheet total is field contamination, not a
    # coincidence (D05 came back with total_revenue == total_asset).
    for field, group, name in (("total_asset", BS, "total_asset"),
                               ("total_equity", BS, "total_equity"),
                               ("total_liabilities", BS, "total_liabilities")):
        other = _get(metrics, group, field)
        if revenue is not None and other is not None and revenue == other and revenue != 0:
            issues.append({
                "check": "revenue_equals_balance_sheet_item",
                "severity": "impossible",
                "fields": ["total_revenue"],
                "detail": f"total_revenue == {name} == {revenue:,.0f}",
            })
            break

    # A REIT's income attributable to unitholders is not its unitholders' funds.
    unitholders = _get(metrics, IS, "unitholders")
    if unitholders is not None and equity is not None and unitholders == equity != 0:
        issues.append({
            "check": "unitholders_equals_equity",
            "severity": "impossible",
            "fields": ["unitholders"],
            "detail": f"unitholders == total_equity == {unitholders:,.0f}",
        })

    # Earnings can legitimately exceed revenue when other income sits outside
    # it -- a REIT's fair-value gains do (J69U FY2026: net 244.1M against
    # revenue 223.0M with 107.7M of non-operating income). Revenue that
    # reconciles with cost + gross is corroborated independently, so nulling it
    # would throw away the better-established figure.
    other_income = _get(metrics, IS, "non_operating_income_or_loss") or 0.0
    corroborated = (None not in (revenue, cost, gross)
                    and _close(gross, revenue - cost))
    if revenue is not None and net_income is not None and not corroborated:
        ceiling = revenue + max(0.0, other_income)
        if net_income > 0 and revenue > 0 and net_income > ceiling * (1 + IDENTITY_TOLERANCE):
            issues.append({
                "check": "earnings_exceed_revenue",
                "severity": "impossible",
                "fields": ["total_revenue", "net_income"],
                "detail": f"net_income={net_income:,.0f} > total_revenue={revenue:,.0f}",
            })

    # Signs that cannot flip.
    for group, field in ((IS, "total_revenue"), (BS, "total_asset"),
                         (BS, "total_current_asset"), (BS, "total_non_current_asset"),
                         (BS, "total_liabilities"), (CF, "capital_expenditure")):
        value = _get(metrics, group, field)
        if value is not None and value < 0:
            issues.append({
                "check": "negative_value",
                "severity": "impossible",
                "fields": [field],
                "detail": f"{field}={value:,.0f} cannot be negative",
            })

    # Scale errors: a misread unit row multiplies everything by 1,000.
    for group, values in metrics.items():
        for field, value in (values or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                share_field = field in ("basic_shares_outstanding",
                                        "diluted_shares_outstanding")
                ceiling = IMPLAUSIBLE_SHARES if share_field else IMPLAUSIBLE
                if abs(value) > ceiling:
                    issues.append({
                        "check": "implausible_magnitude",
                        "severity": "impossible",
                        "fields": [field],
                        "detail": f"{field}={value:,.0f} exceeds any SGX issuer",
                    })

    # One number read once and written into several fields. This is what a
    # wrong-line read looks like from the outside: the model is confident
    # because it copied the figure accurately, just from the wrong row.
    # Only ACROSS statements. Within one statement, equal values are ordinary:
    # total liabilities equals current liabilities when there are no non-current
    # ones, EBIT equals pretax income when there are no finance costs, a REIT's
    # net income equals the amount attributable to unitholders. Across
    # statements there is no such story -- a period flow and a balance-sheet
    # stock are not the same number to the dollar.
    seen: dict[float, set[str]] = {}
    labels: dict[float, list[str]] = {}
    for group, values in metrics.items():
        for field, value in (values or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                if abs(value) >= DUPLICATE_FLOOR:
                    seen.setdefault(value, set()).add(group)
                    labels.setdefault(value, []).append(f"{group}.{field}")
    for value, groups in seen.items():
        if len(groups) < 2:
            continue
        fields = sorted({label.split(".", 1)[1] for label in labels[value]})
        if frozenset(fields) in ALLOWED_EQUAL_PAIRS:
            continue
        issues.append({
            "check": "duplicate_value_across_statements",
            "severity": "impossible",
            "fields": fields,
            "detail": f"{value:,.0f} appears in {', '.join(sorted(labels[value]))}",
        })

    # Field-value contamination: one number written into most of the row,
    # regardless of what each field means. This is `duplicate_value_across_statements`
    # taken to its logical conclusion -- that check only fires above a
    # DUPLICATE_FLOOR and only when the repeats span multiple statement groups,
    # which misses a smaller-scale version of exactly the same failure: a model
    # that reads one figure correctly and then pastes it into every field it
    # couldn't otherwise fill, including fields within the same statement.
    # Zero is excluded -- several fields are legitimately and independently
    # zero (no minorities, no perpetual securities, no property sales) far more
    # often than any other specific number would coincide by chance, so a
    # shared zero isn't evidence of contamination the way a shared 271,700 is.
    CONTAMINATION_MIN_FIELDS = 4
    CONTAMINATION_MIN_SHARE = 0.5
    numeric = [
        (group, field, value)
        for group, values in metrics.items()
        for field, value in (values or {}).items()
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value != 0
    ]
    if numeric:
        by_value: dict[float, list[tuple[str, str]]] = {}
        for group, field, value in numeric:
            by_value.setdefault(value, []).append((group, field))
        for value, locs in by_value.items():
            if len(locs) >= CONTAMINATION_MIN_FIELDS and len(locs) / len(numeric) >= CONTAMINATION_MIN_SHARE:
                issues.append({
                    "check": "field_value_contamination",
                    "severity": "impossible",
                    "fields": sorted({f for _, f in locs}),
                    "detail": f"{value:,.0f} repeats in {len(locs)}/{len(numeric)} numeric fields "
                              f"({', '.join(f'{g}.{f}' for g, f in locs)})",
                })

    # A row where every reported number is exactly zero is not a company that
    # had a quiet half -- revenue, assets and liabilities are never *all* nil
    # for an issuer that filed results at all. This is the same signal
    # `core_fields_present` (financial_statement_rag.py) uses to reject an
    # extraction before it's ever written; this is the retrospective version.
    all_values = [
        value
        for values in metrics.values()
        for value in (values or {}).values()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    ]
    if len(all_values) >= 10 and all(v == 0 for v in all_values):
        issues.append({
            "check": "all_zero_row",
            "severity": "impossible",
            "fields": sorted({f for values in metrics.values() for f, v in (values or {}).items() if v == 0}),
            "detail": f"all {len(all_values)} reported fields are exactly zero",
        })

    # Share counts: basic and diluted differ by a few percent, never orders.
    basic = _get(metrics, IS, "basic_shares_outstanding")
    diluted = _get(metrics, IS, "diluted_shares_outstanding")
    if basic and diluted and basic > 0 and diluted > 0:
        if max(basic, diluted) / min(basic, diluted) > 10:
            # Diluted and basic differ by a few percent in reality, so a 10x gap
            # means one was misscaled — indict the larger, keep the plausible one.
            bad = ("basic_shares_outstanding" if basic > diluted
                   else "diluted_shares_outstanding")
            issues.append({
                "check": "share_counts_diverge",
                "severity": "impossible",
                "fields": [bad],
                "detail": f"basic={basic:,.0f} vs diluted={diluted:,.0f}",
            })

    order = {"impossible": 0, "identity": 1}
    return sorted(issues, key=lambda i: order.get(i["severity"], 9))


def check_sources(provenance: dict | None) -> list[dict]:
    """Rejects values taken from a line that cannot be the metric claimed.

    `provenance` carries the printed label the model read each figure from, so a
    value lifted out of a commitments or contingent-liabilities note is caught
    even when its arithmetic happens to look plausible.
    """
    issues = []
    for group, fields in (provenance or {}).items():
        for field, detail in (fields or {}).items():
            label = str((detail or {}).get("source_label") or "").lower()
            if not label or (detail or {}).get("value") is None:
                continue
            hit = next((t for t in FORBIDDEN_SOURCE_TERMS if t in label), None)
            if hit:
                issues.append({
                    "check": "value_from_forbidden_line",
                    "severity": "impossible",
                    "fields": [field],
                    "detail": f"{field} read from {(detail or {}).get('source_label')!r}"
                              f" (contains {hit!r})",
                })
    return issues


def apply(metrics: dict, provenance: dict | None = None) -> tuple[dict, list[dict]]:
    """Nulls fields a rule proved wrong; identity breaks are reported only.

    An identity break tells you one of several components is wrong without
    saying which, so removing all of them would throw away good figures; those
    rows are flagged for review instead.
    """
    issues = check(metrics) + check_sources(provenance)
    cleaned = {group: dict(values or {}) for group, values in metrics.items()}

    for issue in issues:
        if issue["severity"] != "impossible":
            continue
        for field in issue["fields"]:
            for group in cleaned:
                if field in cleaned[group] and cleaned[group][field] is not None:
                    # "earnings exceed revenue" indicts the pair; drop the
                    # smaller-confidence one (revenue) rather than both.
                    if issue["check"] == "earnings_exceed_revenue" and field == "net_income":
                        continue
                    cleaned[group][field] = None
    return cleaned, issues


def summarise(issues: list[dict]) -> str:
    if not issues:
        return "ok"
    return "; ".join(f"{i['check']}({i['detail']})" for i in issues[:3])


# --- continuity against the symbol's own history -------------------------
# Every other rule here is within a single row, so a mistake that keeps the row
# internally consistent is invisible to them. The clearest example is a
# liabilities/equity swap: assets = liabilities + equity still holds, the split
# checks still hold, and only the issuer's other periods disagree. Equity and
# liabilities are stable across half-years, so the series is the evidence.
SERIES_WINDOW = 6
# Combined |ln| deviation of the unswapped assignment needed before the history
# is allowed to override the extraction.
SWAP_MIN_DEVIATION = 1.0
# Both figures are within ~2x of their swapped medians after the swap.
SWAP_MAX_RESIDUAL = 0.7
# The swap must fit the history at least this much better than the reading did.
SWAP_GAIN = 4.0
# A single line this far from the symbol's median is worth reporting.
SERIES_OUTLIER = 5.0
# Cost of revenue as a share of revenue is a property of the business, not of the
# period, so a row whose ratio is far from the symbol's own is a misread scale or
# a wrong column (QES.SI held cost/revenue 0.04 where every other period is 0.76).
COST_RATIO_TOL = 3.0


def _ln_dev(value, median) -> float:
    if value is None or median is None or value <= 0 or median <= 0:
        return float("inf")
    return abs(math.log(value / median))


def series_guard(metrics: dict, history: list[dict]) -> tuple[dict, list[dict]]:
    """Undoes a liabilities/equity swap the symbol's own history disproves.

    `history` is the stored balance sheet of the same symbol's other periods
    (dicts with total_asset / total_liabilities / total_equity). Two figures that
    are far from their own medians but close to each other's are swapped, which
    is a mapping error that leaves every within-row identity intact.
    """
    issues: list[dict] = []
    rows = [h for h in (history or []) if isinstance(h, dict)][-SERIES_WINDOW:]
    liabilities = _get(metrics, BS, "total_liabilities")
    equity = _get(metrics, BS, "total_equity")
    assets = _get(metrics, BS, "total_asset")

    pairs = [(h.get("total_liabilities"), h.get("total_equity")) for h in rows]
    pairs = [(l, e) for l, e in pairs
             if isinstance(l, (int, float)) and isinstance(e, (int, float)) and l > 0 and e > 0]

    if liabilities and equity and liabilities > 0 and equity > 0 and len(pairs) >= 2:
        median_l = statistics.median(l for l, _ in pairs)
        median_e = statistics.median(e for _, e in pairs)
        as_read = _ln_dev(liabilities, median_l) + _ln_dev(equity, median_e)
        swapped = _ln_dev(liabilities, median_e) + _ln_dev(equity, median_l)
        if (swapped <= SWAP_MAX_RESIDUAL
                and as_read >= SWAP_MIN_DEVIATION
                and as_read >= SWAP_GAIN * swapped):
            metrics = {group: dict(values or {}) for group, values in metrics.items()}
            metrics[BS]["total_liabilities"] = equity
            metrics[BS]["total_equity"] = liabilities
            issues.append({
                "check": "liabilities_equity_swapped",
                "severity": "corrected",
                "fields": ["total_liabilities", "total_equity"],
                "detail": f"{liabilities:,.0f}/{equity:,.0f} read as liabilities/equity; "
                          f"history median {median_l:,.0f}/{median_e:,.0f} "
                          f"(deviation {as_read:.2f} -> {swapped:.2f}) - swapped back",
            })
            liabilities, equity = equity, liabilities

    # Cost ratio: reported, never edited -- the ratio alone cannot say which of
    # revenue or cost is wrong.
    revenue = _get(metrics, IS, "total_revenue")
    cost = _get(metrics, IS, "cost_of_revenue")
    if revenue and cost and revenue > 0 and cost > 0:
        ratios = [h["cost_of_revenue"] / h["total_revenue"] for h in rows
                  if isinstance(h.get("total_revenue"), (int, float))
                  and isinstance(h.get("cost_of_revenue"), (int, float))
                  and h["total_revenue"] > 0 and h["cost_of_revenue"] > 0]
        if len(ratios) >= 2:
            median_ratio = statistics.median(ratios)
            ratio = cost / revenue
            if not (median_ratio / COST_RATIO_TOL <= ratio <= median_ratio * COST_RATIO_TOL):
                issues.append({
                    "check": "cost_ratio_outlier",
                    "severity": "review",
                    "fields": ["cost_of_revenue", "total_revenue"],
                    "detail": f"cost/revenue={ratio:.2f} vs symbol median {median_ratio:.2f}",
                })

    # Continuity only: a line far from the symbol's median is reported, never edited.
    asset_history = [h["total_asset"] for h in rows
                     if isinstance(h.get("total_asset"), (int, float)) and h["total_asset"] > 0]
    for field, value, median in (
            ("total_asset", assets,
             statistics.median(asset_history) if asset_history else 0),
            ("total_liabilities", liabilities,
             statistics.median([l for l, _ in pairs]) if pairs else 0),
            ("total_equity", equity,
             statistics.median([e for _, e in pairs]) if pairs else 0)):
        if not value or not median or value <= 0:
            continue
        ratio = value / median
        if ratio >= SERIES_OUTLIER or ratio <= 1 / SERIES_OUTLIER:
            issues.append({
                "check": "series_outlier",
                "severity": "review",
                "fields": [field],
                "detail": f"{field}={value:,.0f} vs symbol median {median:,.0f} ({ratio:.2f}x)",
            })

    return metrics, issues
