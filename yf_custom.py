import yfinance as yf
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter


class YFSession(CacheMixin, LimiterMixin, Session):
    pass


_session = YFSession(
    limiter=Limiter(RequestRate(10, Duration.SECOND * 2)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session)
