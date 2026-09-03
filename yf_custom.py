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


# class YFSession(CacheMixin, LimiterMixin, Session):
class YFSession(curl_requests.Session):    
    pass


_session = YFSession(
    impersonate="chrome",                               # curl_cffi argument
    verify=not bool(_proxy),                            # proxy presents its own CA
    # limiter=Limiter(RequestRate(30, Duration.MINUTE)),  # ~0.5 requests/sec
    # backend=SQLiteCache("yfinance.cache", expire_after=86400),
)
if _proxy:
    _session.proxies = {'http': _proxy, 'https': _proxy}


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session, proxy=_proxy)
