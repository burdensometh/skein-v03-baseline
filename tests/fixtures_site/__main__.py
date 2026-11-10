"""Run the fixture site standalone for demos: ``python -m tests.fixtures_site --port 8000``."""

from __future__ import annotations

import argparse
import asyncio

from tests.fixtures_site.server import FixtureSite, SiteOptions


async def serve(host: str, port: int, options: SiteOptions) -> None:
    site = FixtureSite(options)
    await site.start(host, port)
    print(f"fixture site listening on http://{host}:{site.port}/ (Ctrl-C to stop)", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await site.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="skein fixture site")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--pages", type=int, default=3, help="catalog pages")
    parser.add_argument("--per-page", type=int, default=4, help="products per catalog page")
    parser.add_argument("--crawl-delay", type=float, default=None, help="Crawl-delay announced in robots.txt")
    parser.add_argument("--validators", choices=("none", "etag", "last-modified", "both"), default="both",
                        help="cache validators sent with pages (enables 304 answers)")
    parser.add_argument("--latency", type=float, default=0.0, help="seconds added to every response")
    args = parser.parse_args()
    options = SiteOptions(catalog_pages=args.pages, products_per_page=args.per_page, crawl_delay=args.crawl_delay,
                          validators=args.validators, latency=args.latency)
    try:
        asyncio.run(serve(args.host, args.port, options))
    except KeyboardInterrupt:
        print("stopped")


main()
