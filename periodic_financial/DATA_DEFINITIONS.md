# SGX periodic financials: what each number means

This explains what is in each row of `sgx_periodic_financial` and where every
number comes from. It's about the data, not the code.

## The short version

- **One row = one company, one half-year.** `H1` is the first six months of the
  company's financial year and `H2` is the second six months.
- **Numbers come from the company's own report**: the half-year or full-year
  results announcement filed on SGX. A few rows come from an HKEX or SEC filing
  when SGX never received the half (`source_url` says which document).
- **Some numbers are read off the page, and some are calculated** from numbers
  read off the page. The tables below say which, field by field.
- **Every amount is in SGD and stored as the actual amount**, never in
  thousands or millions. A report printing "S$'000 158,370" is stored as
  `158370000` (S$158.37 million), so there is no scale to guess. Reports in
  USD, RMB, MYR and so on are converted to SGD first.
- **Blank means we could not get it reliably.** We never guess, and a blank is
  never a hidden 0.

## Three kinds of field

| Kind | What it means | Example |
|---|---|---|
| **Printed** | Read straight from a labelled line in the report | Revenue, profit before tax, total equity |
| **Calculated** | Always worked out from other fields with a fixed formula, even when the company prints its own line | Operating income, non-operating income |
| **Printed, else calculated** | We use the printed line. If there isn't one, we calculate it | Total assets, EBIT |

Why calculate something a company already prints? Companies define "operating
profit" in different ways. One includes other income and gains, another
doesn't. A formula gives every company the same definition, so the numbers can
be compared across companies.

## Income statement

| Field | Kind | What it is |
|---|---|---|
| `total_revenue` | Printed | Revenue / turnover. Other income printed outside revenue is **not** included. Banks: total income (net interest + fees + other income), never gross interest income. Insurers: insurance revenue. REITs: gross revenue, plus "other property income" when it sits above net property income |
| `cost_of_revenue` | Printed | Cost of sales / direct costs / property operating expenses. Blank when the company lists its expenses by type (staff, depreciation, materials) instead of a cost of sales line |
| `gross_income` | Printed, else calculated | Gross profit (net property income for REITs). If no labelled line: revenue − cost of revenue |
| `operating_expense` | Printed | Operating expenses **excluding** cost of sales and finance costs. Never includes FX, fair-value changes, associates, impairment of investments or tax |
| `operating_income` | **Calculated** | Gross profit (or revenue, when there's no cost of sales) − operating expenses. This is the SGX annual table's definition. **Banks are the exception**: we use their printed "profit after allowances" line |
| `non_operating_income_or_loss` | **Calculated** | Profit before tax − operating income + finance costs. In plain words: **everything between operating profit and profit before tax, except finance costs**. That's other income, FX gains/losses, fair-value changes, share of associates and JVs, and one-off items |
| `interest_expense_non_operating` | Printed | Finance costs, as a positive number. Blank for banks, whose interest expense is already inside their income |
| `pretax_income` | Printed | Profit before tax |
| `income_taxes` | Printed | Tax expense as a positive number (a tax credit is negative) |
| `net_income` | Printed | Profit for the period, **before** it is split between owners, minorities and perpetual holders. Includes discontinued operations |
| `ebit` | Printed, else calculated | A line labelled EBIT / PBIT. If none: profit before tax + finance costs |
| `ebitda` | Printed only | Only when the company prints an EBITDA figure: on the statement, highlights, commentary or segment note. Never calculated (see below) |
| `funds_from_operation` | Printed only | FFO, almost never printed |
| `net_property_sales` | Printed only | Gain/loss on selling investment properties |
| `unitholders` | Printed | Profit attributable to owners / unitholders. If the report shows no "attributable to" split, it's all of net income |
| `minorities` | Printed | Profit attributable to non-controlling interests |
| `perpetual_security_holders` | Printed | Profit attributable to perpetual securities holders |
| `basic_shares_outstanding`, `diluted_shares_outstanding` | Printed only | Weighted average share/unit count from the EPS note, for this half only. Blank for H2 rows calculated from the full year (see below), because a 12-month average can't be split |

**Insurers (G07, K6S, U13):** operating income is the insurance service
result (the underwriting profit) minus other expenses. Non-operating income is
mostly the net investment result, which is core business for an insurer, so
don't read a large non-operating share as low-quality earnings.

**Prudential (K6S)** prints two pretax lines. We use "Profit before tax",
which includes the tax charged on policyholders' returns, together with the
total tax charge. Its headline "Profit before tax attributable to
shareholders' returns" excludes it from both. Net income is the same either
way.

### How the income statement fits together

```
  Revenue
− Cost of revenue
= Gross income
− Operating expenses
= Operating income                     ← calculated
+ Non-operating income or loss         ← calculated: the gap down to pretax
− Finance costs
= Profit before tax                    ← printed
− Income tax
= Net income                           ← printed
  split into unitholders + minorities + perpetual holders
```

Example from a real report (Metro Holdings, 6 months to Sep 2024, S$'000):

| Line | Amount | Where it goes |
|---|---|---|
| Revenue | 48,350 | `total_revenue` |
| Cost of revenue | (45,829) | `cost_of_revenue` |
| General and admin expenses | (10,237) | `operating_expense` |
| → operating income | **(7,716)** | 48,350 − 45,829 − 10,237 |
| Other net income | 24,650 | non-operating |
| Share of associates | (6,418) | non-operating |
| Share of joint ventures | 12,373 | non-operating |
| → non-operating income | **30,605** | 24,650 − 6,418 + 12,373 |
| Finance costs | (15,885) | `interest_expense_non_operating` |
| Profit before tax | 7,004 | (7,716) + 30,605 − 15,885 ✓ |

### EBITDA

EBITDA is only filled when the company prints it, so many rows are blank.
When it is printed:

- Plain EBITDA wins over "adjusted" or "underlying" EBITDA. If only adjusted
  EBITDA is printed, we use that.
- A segment note's EBITDA counts only when it covers the whole group. A total
  of the business segments with corporate costs listed separately below it
  doesn't count.
- An H2 calculated from the full year (full year − H1) is only filled when both
  figures are on the same basis. For example, if the full year is "continuing
  operations" and the H1 isn't, H2 stays blank.

## Balance sheet

All amounts are as at the half-year's end date. Assets and liabilities are
positive numbers.

| Field | Kind | What it is |
|---|---|---|
| `total_asset` | Printed, else calculated | Total assets. If not printed: liabilities + equity, or current + non-current assets |
| `total_current_asset` | Printed, else calculated | Total current assets, **including** assets held for sale. If not printed: total assets − non-current assets |
| `total_non_current_asset` | Printed, else calculated | Same idea |
| `total_liabilities` | Printed, else calculated | If not printed: current + non-current liabilities, or total assets − equity |
| `total_current_liabilities` | Printed, else calculated | Including liabilities held for sale |
| `total_non_current_liabilities` | Printed, else calculated | Same idea |
| `total_equity` | Printed | **All** of the group's equity: owners + perpetual securities + non-controlling interests. For REITs, "net assets". Never just the owners' share |
| `working_capital` | Printed, else calculated | Net current assets. If not printed: current assets − current liabilities |

Banks and some others don't split current and non-current, so those four
fields can be blank while the totals are filled.

## Cash flow

Outflows are negative, except capital expenditure, which is a positive amount
spent.

| Field | Kind | What it is |
|---|---|---|
| `operating_cash_flow` | Printed | Net cash from operating activities |
| `investing_cash_flow` | Printed | Net cash from investing activities |
| `financing_cash_flow` | Printed | Net cash from financing activities |
| `capital_expenditure` | **Calculated** | Spending on building or improving assets: PP&E + intangibles + spending on investment properties the group owns or is building + construction in progress + land rights + mine/exploration/biological assets. **Buying** a property or a company is **not** capex. A line that shows money coming in (a net disposal) is left out |
| `net_cash_flow` | Printed, with one adjustment | Net increase/(decrease) in cash, before the FX effect on cash. When a company prints the FX line above the net line, we take it out, so this is always operating + investing + financing |
| `free_cash_flow` | Printed only | Only when the company prints a free cash flow line. In practice it's always blank |

## H1 and H2 rows

- **H1** is read directly from the half-year report.
- **H2** is read from the six-month column of the full-year report when the
  company prints one.
- **When the company only prints the full year**, H2 = full year − H1. This
  works for the income statement and cash flow. The balance sheet doesn't need
  it, because it's a snapshot at the year-end date.
  - Share counts are never calculated this way and stay blank.
  - Operating income, non-operating income and EBIT are recalculated from H2's
    own figures, not subtracted.
  - If H1 and H2 look like different businesses (H2 many times bigger), the
    row is flagged for review. After checking, real cases like an acquisition
    are kept and listed in `config/comparable_first_halves.json`.
- `financial_year`: a year ending January–June is labelled as the previous
  calendar year (Singtel's year to 31 Mar 2026 is FY2025). An H1 takes the
  label of the year it belongs to.

## Currency

- Every amount is converted to SGD at the exchange rate of the quarter end on
  or before the period end.
- For a calculated H2, the subtraction is done in the report's own currency
  first and the result is then converted. Exchange-rate moves between H1 and
  the year end don't leak into H2.

## Checks on every row

- **Impossible values are blanked**, never kept. Examples: negative revenue or
  assets, a figure too big for any SGX company (usually a misread scale), or
  one number copied into fields on different statements.
- **Broken totals are flagged, not changed.** For example, assets ≠ liabilities
  + equity, or the three cash flows not adding up to the net change.
- **H1 + H2 is compared with the SGX annual table**, which catches a wrong half.
- **Manual corrections are listed in files with their source**, e.g.
  `data/doc/audit_adjustments.json` for a later audited restatement.

## Differences from the old pipeline

| Field | Old pipeline | Now |
|---|---|---|
| `operating_income` | Whatever line the model picked | Always gross profit − operating expenses, one definition for everyone |
| `non_operating_income_or_loss` | The model added up "other income, FX, fair value, associates…" itself, differently each time. Most stored values didn't even agree with the row's own pretax and operating income | Calculated as the gap between operating income and pretax, so it always agrees |
| `ebit` | Often a copy of operating income | Printed EBIT, else pretax + finance costs |
| `ebitda` | Sometimes calculated or taken from the wrong period | Only when printed, read from wherever the company prints it |
| H2 rows | Sometimes stored a full year or a quarter as H2 | Six-month column, or full year − H1, checked against the annual table |
