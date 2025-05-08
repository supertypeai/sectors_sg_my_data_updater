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


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session, proxy=_proxy)
