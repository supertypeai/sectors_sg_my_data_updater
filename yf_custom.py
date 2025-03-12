import yfinance as yf
from dotenv import load_dotenv
from pyrate_limiter import Duration, RequestRate, Limiter
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

load_dotenv()

_proxy = None


class YFSession(CacheMixin, LimiterMixin, Session):
    pass


_session = YFSession(
    limiter=Limiter(RequestRate(500, Duration.SECOND * 2)),  # max 500 requests per 2 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)


class Ticker(yf.Ticker):
    def __init__(self, ticker):
        super().__init__(ticker, session=_session, proxy=_proxy)
