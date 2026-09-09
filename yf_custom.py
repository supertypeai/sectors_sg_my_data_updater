import yfinance as yf
from dotenv import load_dotenv
from pyrate_limiter import Duration, RequestRate, Limiter
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin
from curl_cffi import requests as curl_requests

load_dotenv()

_proxy = None


# class YFSession(CacheMixin, LimiterMixin, Session):
class YFSession(curl_requests.Session):    
    pass


_session = YFSession(
    impersonate="chrome",                               # curl_cffi argument
    # limiter=Limiter(RequestRate(30, Duration.MINUTE)),  # ~0.5 requests/sec
    # backend=SQLiteCache("yfinance.cache", expire_after=86400),
)


# ---- Request-count reduction (yfinance auth overhead) ----
# yfinance 0.2.59's _get_cookie_and_crumb_basic() hits https://fc.yahoo.com on
# EVERY data request to "refresh" the cookie, even when the A3 cookie is already
# in the session jar (valid for hours). That is ~6-9 wasted calls per symbol
# (~50% of all traffic). Skip the refresh when A3 is already present; if it ever
# goes stale, a 401 toggles yfinance's strategy -> _cookie reset -> fresh fetch.
from yfinance.data import YfData


def _patch_cookie_refresh():
    """Idempotently wrap YfData's cookie+crumb fetch to skip redundant fc.yahoo.com hits."""
    inst = YfData._instances.get(YfData)
    if inst is None or getattr(inst, "_skip_cookie_patched", False):
        return
    inst._skip_cookie_patched = True

    def _patched(timeout=30):
        # A3 present => cookie already valid => skip fc.yahoo.com refresh.
        if "A3" not in inst._session.cookies.get_dict():
            inst._get_cookie_basic(timeout)
        return inst._get_crumb_basic(timeout)

    inst._get_cookie_and_crumb_basic = _patched


# .info also fans out to a fundamentals-timeseries call purely for
# trailingPegRatio (unused here) - skip it to save 1 call/symbol.
def _patch_skip_timeseries():
    from yfinance.scrapers.quote import Quote
    if getattr(Quote, "_skip_timeseries_patched", False):
        return
    Quote._skip_timeseries_patched = True

    def _noop_complementary(self):
        self._already_fetched_complementary = True

    Quote._fetch_complementary = _noop_complementary


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session, proxy=_proxy)
        _patch_cookie_refresh()
        _patch_skip_timeseries()
