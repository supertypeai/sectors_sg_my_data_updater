import yfinance as yf
import os
from dotenv import load_dotenv
from pyrate_limiter import Duration, RequestRate, Limiter
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin
from curl_cffi import requests as curl_requests

load_dotenv()

# CI sets PROXY secret; apply it so Yahoo calls go through the proxy (datacenter
# IPs without it get 401/429-blocked at scale). Two transport layers need it:
#  1. yfinance's internal requests session (info/dividends/crumb) -> set_config
#  2. the curl_cffi session used by ticker.history() -> session.proxies
_proxy = os.environ.get("PROXY") or None
if _proxy:
    yf.set_config(proxy=_proxy)


# class YFSession(CacheMixin, LimiterMixin, Session):
class YFSession(curl_requests.Session):    
    pass


_session = YFSession(
    impersonate="chrome",                               # curl_cffi argument
    # limiter=Limiter(RequestRate(30, Duration.MINUTE)),  # ~0.5 requests/sec
    # backend=SQLiteCache("yfinance.cache", expire_after=86400),
)
if _proxy:
    _session.proxies = {'http': _proxy, 'https': _proxy}


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session, proxy=_proxy)
