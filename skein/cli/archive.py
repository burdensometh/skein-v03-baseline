"""``skein warc``: list, index, verify, extract and replay archived records."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Iterable, TextIO

from skein.config import load_config
from skein.config.runtime import RuntimeSettings
from skein.store.sqlite import Store
from skein.warc.cdx import CDX_HEADER, MATCH_TYPES, CdxIndex, build_index, cdxj_line, database_lines, write_index
from skein.warc.reader import WarcReader, read_record
from skein.warc.record import WarcFormatError
from skein.warc.replay import ReplayError, Replayer
from skein.warc.server import ReplayServer

EXIT_OK = 0
EXIT_PROBLEMS = 1
INDEX_NAME = "index.cdx"


def warc_files(paths: Iterable[str]) -> list[Path]:
    """Expand directories into their ``*.warc.gz`` / ``*.warc`` files (sorted)."""
    found: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            found.extend(sorted(p for p in path.iterdir() if p.name.endswith((".warc.gz", ".warc"))))
        elif path.exists():
            found.append(path)
        else:
            raise ValueError(f"{path} does not exist")
    return found


def archive_directory(args: argparse.Namespace) -> Path:
    if args.dir is not None:
        return Path(args.dir)
    if args.config is not None:
        config = load_config(args.config)
        return config.runtime.archive_directory(str(config.database))
    if args.db is not None:
        return RuntimeSettings().archive_directory(args.db)
    raise ValueError("give --dir, --config or --db")


def _list(paths: list[Path], as_json: bool, out: TextIO) -> int:
    problems = 0
    for path in paths:
        reader = WarcReader(path)
        try:
            for entry in reader:
                record = entry.record
                row = {"file": path.name, "offset": entry.offset, "length": entry.length, "type": record.type,
                       "date": record.date, "uri": record.target_uri, "id": record.record_id,
                       "content_type": record.get("Content-Type"), "bytes": len(record.block)}
                if as_json:
                    out.write(json.dumps(row, sort_keys=True) + "\n")
                else:
                    out.write(f"{path.name} {entry.offset:>10} {entry.length:>7} {record.type:<9} {record.date} "
                              f"{record.target_uri or '-'}\n")
        except WarcFormatError as exc:
            problems += 1
            print(f"skein: {exc}", file=sys.stderr)
        if reader.truncated is not None:
            problems += 1
            out.write(f"{path.name}: truncated record at offset {reader.truncated.offset} "
                      f"({reader.truncated.available} byte(s): {reader.truncated.reason})\n")
    return EXIT_PROBLEMS if problems else EXIT_OK


def _verify(paths: list[Path], out: TextIO) -> int:
    bad = 0
    for path in paths:
        reader = WarcReader(path)
        checked = 0
        try:
            for entry in reader:
                checked += 1
                for problem in entry.record.verify():
                    bad += 1
                    out.write(f"{path.name} @{entry.offset}: {problem} ({entry.record.record_id})\n")
        except WarcFormatError as exc:
            bad += 1
            out.write(f"{path.name}: {exc}\n")
        if reader.truncated is not None:
            bad += 1
            out.write(f"{path.name}: truncated tail at offset {reader.truncated.offset}\n")
        out.write(f"{path.name}: {checked} record(s) checked\n")
    return EXIT_PROBLEMS if bad else EXIT_OK


def _index(args: argparse.Namespace, out: TextIO) -> int:
    paths = warc_files(args.paths)
    lines = build_index(paths)
    if args.output == "-":
        if args.cdxj:
            out.writelines(cdxj_line(line) + "\n" for line in lines)
        else:
            out.write(CDX_HEADER + "\n")
            out.writelines(line.to_line() + "\n" for line in lines)
        return EXIT_OK
    destination = Path(args.output) if args.output else (
        Path(args.paths[0]) / INDEX_NAME if Path(args.paths[0]).is_dir() else Path(INDEX_NAME))
    write_index(lines, destination, cdxj=args.cdxj)
    out.write(f"indexed {len(lines)} capture(s) from {len(paths)} file(s) into {destination}\n")
    return EXIT_OK


def _extract(args: argparse.Namespace, out: TextIO) -> int:
    record = read_record(Path(args.file), args.offset)
    if args.part == "headers":
        text = "".join(f"{name}: {value}\n" for name, value in record.headers)
        out.write(f"{record.version}\n{text}")
        return EXIT_OK
    data = record.payload if args.part == "payload" else record.block
    if args.output:
        Path(args.output).write_bytes(data)
        out.write(f"wrote {len(data)} byte(s) of {record.type} record {record.record_id} to {args.output}\n")
    elif out is sys.stdout:
        out.flush()
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
    else:
        out.write(data.decode("utf-8", "replace"))
    return EXIT_OK


def _replay(args: argparse.Namespace, out: TextIO) -> int:
    directory = archive_directory(args)
    index = _load_index(args, directory)
    try:
        replayed = Replayer(index, directory).replay(args.url, args.at)
    except ReplayError as exc:
        raise ValueError(str(exc)) from exc
    out.write(f"HTTP {replayed.status} {replayed.reason}  ({replayed.filename} @{replayed.offset}, "
              f"{replayed.date}{', via revisit' if replayed.revisit_chain else ''})\n")
    for name, value in replayed.headers:
        out.write(f"{name}: {value}\n")
    if args.output:
        Path(args.output).write_bytes(replayed.body)
        out.write(f"\nbody: {len(replayed.body)} byte(s) written to {args.output}\n")
    elif not args.headers_only:
        out.write("\n" + replayed.body.decode("utf-8", "replace") + "\n")
    return EXIT_OK


def _query(args: argparse.Namespace, out: TextIO) -> int:
    directory = archive_directory(args)
    lines = _load_index(args, directory).query(args.url, match=args.match, since=args.since, until=args.until,
                                               status=args.status, limit=args.limit)
    for line in lines:
        out.write((cdxj_line(line) if args.cdxj else line.to_line()) + "\n")
    return EXIT_OK if lines else EXIT_PROBLEMS


def _load_index(args: argparse.Namespace, directory: Path) -> CdxIndex:
    """An explicit or ``index.cdx`` index; else the database's record locations; else the WARC files read."""
    index_path = Path(args.index) if args.index else directory / INDEX_NAME
    if index_path.exists():
        return CdxIndex.load(index_path)
    database = Path(args.db) if args.db else (load_config(args.config).database if args.config else None)
    if database is not None and database.exists():
        with Store(database) as store:
            lines = list(database_lines(store.db))
        if lines:
            return CdxIndex(lines)
    return CdxIndex(build_index(warc_files([str(directory)])))


async def _serve(server: ReplayServer, duration: float | None) -> None:
    await server.start()
    print(f"replaying on http://{server.host}:{server.port}/web/latest/<url>", flush=True)
    try:
        if duration is None:
            await asyncio.Event().wait()
        else:
            await asyncio.sleep(duration)
    finally:
        await server.stop()


def cmd_warc(args: argparse.Namespace, out: TextIO) -> int:
    if args.action == "list":
        return _list(warc_files(args.paths), args.json, out)
    if args.action == "verify":
        return _verify(warc_files(args.paths), out)
    if args.action == "index":
        return _index(args, out)
    if args.action == "extract":
        return _extract(args, out)
    if args.action == "query":
        return _query(args, out)
    if args.action == "serve":
        directory = archive_directory(args)
        server = ReplayServer(Replayer(_load_index(args, directory), directory), host=args.host, port=args.port,
                              rewrite=not args.no_rewrite)
        try:
            asyncio.run(_serve(server, args.duration))
        except KeyboardInterrupt:
            out.write(f"served {server.served} capture(s)\n")
        return EXIT_OK
    return _replay(args, out)


def add_warc_parser(commands: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    warc = commands.add_parser("warc", help="list, index, verify, extract or replay WARC records")
    actions = warc.add_subparsers(dest="action", required=True, metavar="ACTION")
    listing = actions.add_parser("list", help="one line per record")
    listing.add_argument("paths", nargs="+", help="WARC files or directories")
    listing.add_argument("--json", action="store_true")
    verify = actions.add_parser("verify", help="check block and payload digests and the file structure")
    verify.add_argument("paths", nargs="+")
    index = actions.add_parser("index", help="write a CDX index of response and revisit records")
    index.add_argument("paths", nargs="+")
    index.add_argument("-o", "--output", help="index file ('-' for stdout; default: index.cdx in the directory)")
    index.add_argument("--cdxj", action="store_true", help="write CDXJ (urlkey timestamp JSON) instead of CDX")
    extract = actions.add_parser("extract", help="print or save one record found by file and offset")
    extract.add_argument("file")
    extract.add_argument("--offset", type=int, required=True)
    extract.add_argument("--part", choices=("headers", "block", "payload"), default="payload")
    extract.add_argument("-o", "--output")
    replay = actions.add_parser("replay", help="rebuild the archived response for a URL")
    replay.add_argument("url")
    replay.add_argument("--at", help="capture closest to this 14-digit timestamp (default: latest)")
    replay.add_argument("--headers-only", action="store_true")
    replay.add_argument("-o", "--output", help="write the decoded body to this file")
    query = actions.add_parser("query", help="captures matching a URL, a URL prefix, a host or a domain")
    query.add_argument("url")
    query.add_argument("--match", choices=MATCH_TYPES, default="exact")
    query.add_argument("--since", help="timestamp prefix, e.g. 2024 or 20240501")
    query.add_argument("--until", help="timestamp prefix")
    query.add_argument("--status", help="only captures with this HTTP status")
    query.add_argument("--limit", type=int)
    query.add_argument("--cdxj", action="store_true", help="print CDXJ instead of CDX lines")
    serve = actions.add_parser("serve", help="replay captures over HTTP at /web/<timestamp>/<url>")
    serve.add_argument("--no-rewrite", action="store_true", help="serve HTML exactly as archived")
    for command in (replay, query, serve):
        origin = command.add_mutually_exclusive_group()
        origin.add_argument("--dir", help="archive directory")
        origin.add_argument("--config", help="take the archive directory from this configuration")
        origin.add_argument("--db", help="archive directory next to this database")
        command.add_argument("--index", help="CDX index (default: index.cdx in the directory, else built on the fly)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8090)
    serve.add_argument("--duration", type=float, help="stop after this many seconds")
