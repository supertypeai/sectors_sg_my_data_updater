"""
Daily incremental SGX corporate-actions update.

Complements sgx_corporate_actions.py, which does the full-history backfill. This
wraps it in the two-horizon schedule:

  daily   narrow notices window  -> cheap, catches amendments and the forwards
  weekly  full notices history   -> catches anything announced outside a window

Why the daily run is cheap (measured, not estimated):
  a 30-day CACT window is 76 filings on 1 page, versus 11,328 filings over 46
  pages for 2014+. The window also names the only symbols that can have changed
  (54 of 620), so the corporateactions sweep shrinks with it.

Usage:
    python sgx_scrape/sgx_daily_run.py                  # dry run, daily mode
    python sgx_scrape/sgx_daily_run.py --weekly         # dry run, weekly mode
    python sgx_scrape/sgx_daily_run.py --write --yes    # the cron job
"""

import argparse
import datetime
import logging
import os
import subprocess
import sys

HERE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE_DIR, "sgx_corporate_actions.py")

# A first run has an empty forward calendar and no history of amendments, so it
# seeds a wider window. Steady state only needs to re-read the recent past.
DAILY_WINDOW = 30
SEED_WINDOW = 180
# No notices window = read the scraper's full 2014+ history (weekly reconcile).
FULL_HISTORY = None

_log = logging.getLogger("sgx_daily_run")


def window(weekly, seeding):
    if weekly:
        return FULL_HISTORY
    return SEED_WINDOW if seeding else DAILY_WINDOW


def build_command(window_days, write, yes):
    # sgx_upcoming_dividend is left entirely to sgx_buyback_pipeline: it is the
    # feed behind the "Dividend Announcement" news stream and
    # sgx_company_report.upcoming_dividends. This run owns only the four
    # corporate-action tables, so --split-calendar/--prune-calendar are absent -
    # adding them would put two writers on that table.
    cmd = [sys.executable, SCRIPT, "--all"]
    if window_days is None:
        _log.info("weekly mode: full notices history")
    else:
        cmd += ["--notices-window", str(window_days)]
        if window_days != DAILY_WINDOW:
            _log.info("seed mode: %d-day notices window", window_days)
    if write:
        cmd += ["--write"]
        if yes:
            cmd += ["--yes"]
    return cmd


def main():
    ap = argparse.ArgumentParser(description="Daily incremental SGX corporate actions")
    ap.add_argument("--weekly", action="store_true",
                    help="full reconcile instead of the narrow window")
    ap.add_argument("--seed", action="store_true",
                    help="first run: use the wide %d-day window" % SEED_WINDOW)
    ap.add_argument("--notices-window", type=int, metavar="DAYS",
                    help="override the notices window (wins over --seed)")
    ap.add_argument("--write", action="store_true", help="upsert to Supabase")
    ap.add_argument("--yes", action="store_true", help="confirm the write")
    args = ap.parse_args()

    if args.yes and not args.write:
        # Otherwise the run dry-runs and exits 0, and a typo in a cron line
        # looks successful while writing nothing.
        ap.error("--yes requires --write (you asked to confirm a write that is not requested)")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # The scraper writes to Postgres over plain requests; keep its httpx chatter out.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    days = args.notices_window if args.notices_window is not None else window(args.weekly, args.seed)
    cmd = build_command(days, args.write, args.yes)
    _log.info("running: %s", " ".join(cmd))
    started = datetime.datetime.now()

    # argv list, no shell (no injection surface); env copied explicitly so the
    # child inherits SUPABASE_* and the worker counts rather than relying on
    # an implicit inheritance.
    result = subprocess.run(cmd, env=os.environ.copy())
    elapsed = (datetime.datetime.now() - started).total_seconds()

    if result.returncode != 0:
        # Non-zero is deliberate: a failed CACT detail fetch aborts rather than
        # writing a partial batch, so the next run must retry, not skip.
        _log.error("FAILED after %.0fs (exit %d)", elapsed, result.returncode)
        return result.returncode
    _log.info("done in %.0fs", elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
