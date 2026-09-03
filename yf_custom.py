import yfinance as yf
import os
from dotenv import load_dotenv
from pyrate_limiter import Duration, RequestRate, Limiter
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin
from curl_cffi import requests as curl_requests

load_dotenv()

# PROXY secret: proxy TLS-intercepts, so verify off when proxied (like
# sector_scraper_klse.py). Wire both yfinance transport layers.
_proxy = os.environ.get("PROXY") or None
if _proxy:
    yf.set_config(proxy=_proxy)
    from yfinance.data import YfData
    _yd = YfData._instances.get(YfData)
    if _yd is not None:
        _yd._session.verify = False

import random as _random
import time as _time
_MIN_DELAY = 0.2
_MAX_DELAY = 0.8


class YFSession(curl_requests.Session):
    def get(self, *args, **kwargs):
        _time.sleep(_random.uniform(_MIN_DELAY, _MAX_DELAY))
        return super().get(*args, **kwargs)


_session = YFSession(
    impersonate="chrome",                               # curl_cffi argument
    verify=not bool(_proxy),
    # limiter=Limiter(RequestRate(30, Duration.MINUTE)),  # ~0.5 requests/sec
    # backend=SQLiteCache("yfinance.cache", expire_after=86400),
)
if _proxy:
    _session.proxies = {'http': _proxy, 'https': _proxy}


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session, proxy=_proxy)
