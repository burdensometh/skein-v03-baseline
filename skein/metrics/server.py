"""``GET /metrics`` in the Prometheus text format, served from the worker's event loop.

Each scrape calls ``render`` for a fresh exposition. With a ``health``
callable, ``GET /healthz`` answers its report as JSON -- 200 while
``healthy`` is true, 503 otherwise -- for process supervisors and
orchestrators. Other paths get a 404, other methods a 405.
"""

from __future__ import annotations

import json
from typing import Any, Callable
from urllib.parse import urlsplit

from skein.metrics.prometheus import CONTENT_TYPE
from skein.net.server import Reply, SimpleServer


class MetricsServer(SimpleServer):
    """Serves ``render()`` at ``/metrics``; :attr:`scrapes` counts successful scrapes."""

    def __init__(self, render: Callable[[], str], *, host: str = "127.0.0.1", port: int = 0,
                 health: Callable[[], dict[str, Any]] | None = None) -> None:
        super().__init__(self._answer, host=host, port=port)
        self.render = render
        self.health = health
        self.scrapes = 0

    def _answer(self, method: str, target: str) -> Reply:
        path = urlsplit(target).path
        if path not in ("/metrics", "/healthz") or (path == "/healthz" and self.health is None):
            return Reply.text(404, "Not Found", "not found\n")
        if method not in ("GET", "HEAD"):
            return Reply.text(405, "Method Not Allowed", "method not allowed\n")
        if path == "/healthz":
            assert self.health is not None
            report = self.health()
            status, reason = (200, "OK") if report.get("healthy") else (503, "Service Unavailable")
            return Reply(status, reason, (json.dumps(report, sort_keys=True) + "\n").encode("utf-8"),
                         [("Content-Type", "application/json")])
        self.scrapes += 1
        return Reply(200, "OK", self.render().encode("utf-8"), [("Content-Type", CONTENT_TYPE)])
