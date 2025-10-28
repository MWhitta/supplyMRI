#!/usr/bin/env python3
"""
Command-line utility for collecting S-K 1300 technical reports from EDGAR.
"""

import argparse
from pathlib import Path

from supplymri import EdgarClient
from supplymri.workflows import download_edgar_documents


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search SEC EDGAR for mining disclosures and download matching documents."
    )
    parser.add_argument(
        "--query",
        default="S-K 1300",
        help="Full-text search query submitted to EDGAR (default: %(default)s).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of documents to download (default: %(default)s).",
    )
    parser.add_argument(
        "--forms",
        nargs="*",
        help="Restrict to specific form types (e.g. 10-K EX-96).",
    )
    parser.add_argument(
        "--description-filter",
        help="Only keep documents whose description or type contains this substring.",
    )
    parser.add_argument(
        "--date-range",
        default="all",
        help="EDGAR filed date range filter (all, today, 10d, 1m, 3m, 1y, custom...).",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="Result index to start from (useful for paging through matches).",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        help="Directory where documents will be stored (defaults to the client setting).",
    )
    parser.add_argument(
        "--user-agent",
        help="Override the SEC user agent string (include your contact information).",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip writing JSON metadata alongside each download.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download files even if they already exist locally.",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.3,
        help="Seconds to pause between requests (default: %(default)s).",
    )

    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")
    if args.start < 0:
        parser.error("--start must be non-negative")
    return args


def main() -> None:
    args = parse_args()
    client = EdgarClient(
        user_agent=args.user_agent,
        throttle_seconds=args.throttle,
    )

    print(f"Searching EDGAR for '{args.query}'...")
    documents = client.search_documents(
        args.query,
        limit=args.limit,
        forms=args.forms,
        description_filter=args.description_filter,
        start=args.start,
        date_range=args.date_range,
    )

    if not documents:
        print("No documents matched the query.")
        return

    resolved_destination = client.resolve_destination(args.dest)
    print(f"Found {len(documents)} documents. Downloading to {resolved_destination} ...")
    workflow = download_edgar_documents(
        client,
        documents,
        destination=args.dest,
        include_metadata=not args.no_metadata,
        overwrite=args.overwrite,
    )

    for doc, path in zip(documents, workflow.saved_paths):
        print(f"{path} <- {doc.url}")
    print(f"Saved {workflow.count} documents via EDGAR workflow.")


if __name__ == "__main__":
    main()
