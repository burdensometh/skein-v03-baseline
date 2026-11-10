"""Asyncio HTTP/1.1 client: pooling, keep-alive, chunked, gzip/deflate, redirects, limits."""

from skein.net.client import DEFAULT_USER_AGENT, ClientConfig, HttpClient
from skein.net.errors import (
    BodyTooLarge,
    ConnectError,
    ConnectionClosed,
    DecodingError,
    HttpError,
    InvalidURL,
    ProtocolError,
    RedirectError,
    RedirectLoop,
    TimeoutError,
    TooManyRedirects,
)
from skein.net.headers import Headers
from skein.net.response import RedirectHop, Response

__all__ = [
    "DEFAULT_USER_AGENT",
    "BodyTooLarge",
    "ClientConfig",
    "ConnectError",
    "ConnectionClosed",
    "DecodingError",
    "Headers",
    "HttpClient",
    "HttpError",
    "InvalidURL",
    "ProtocolError",
    "RedirectError",
    "RedirectHop",
    "RedirectLoop",
    "Response",
    "TimeoutError",
    "TooManyRedirects",
]
