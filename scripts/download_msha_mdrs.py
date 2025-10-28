#!/usr/bin/env python3
"""
Command-line utility for collecting MSHA Mine Data Retrieval System datasets.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from supplymri import MshaClient
from supplymri.workflows import download_msha_dataset


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download MSHA MDRS datasets via the Department of Labor API."
    )
    parser.add_argument(
        "--api-key",
        help="DOL Open Data API key. Defaults to the DOL_API_KEY environment variable.",
    )
    parser.add_argument(
        "--agency",
        default="msha",
        help="Agency abbreviation to query (default: %(default)s).",
    )
    parser.add_argument(
        "--endpoint",
        action="append",
        dest="endpoints",
        help="Dataset endpoint name (may be repeated).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of records to retrieve per endpoint (default: %(default)s).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Starting record offset (default: %(default)s).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Number of records per API call (default: %(default)s).",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        help="Directory where datasets will be stored (defaults to the client setting).",
    )
    parser.add_argument(
        "--filter-json",
        help="JSON string passed as the filter_object parameter.",
    )
    parser.add_argument(
        "--filter-file",
        type=Path,
        help="Path to a JSON file containing the filter_object payload.",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional query parameter (may be repeated).",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip writing dataset and chunk metadata files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files if they already exist.",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=MshaClient.DEFAULT_THROTTLE,
        help="Seconds to pause between API requests (default: %(default)s).",
    )
    parser.add_argument(
        "--user-agent",
        help="Custom User-Agent header to send with API requests.",
    )
    parser.add_argument(
        "--list-endpoints",
        action="store_true",
        help="List available endpoints for the selected agency and exit.",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive.")
    if args.offset < 0:
        parser.error("--offset must be non-negative.")
    if args.chunk_size <= 0:
        parser.error("--chunk-size must be positive.")
    if args.throttle < 0:
        parser.error("--throttle must be non-negative.")

    return args


def parse_filter(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    if args.filter_json and args.filter_file:
        raise ValueError("Specify at most one of --filter-json or --filter-file.")
    if args.filter_json:
        try:
            return json.loads(args.filter_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"--filter-json must be valid JSON: {exc}") from exc
    if args.filter_file:
        try:
            text = args.filter_file.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Unable to read filter file: {exc}") from exc
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"--filter-file must contain valid JSON: {exc}") from exc
    return None


def parse_extra_params(param_options: List[str]) -> Dict[str, str]:
    params: Dict[str, str] = {}
    for option in param_options:
        if "=" not in option:
            raise ValueError(f"Invalid --param value '{option}'. Expected KEY=VALUE.")
        key, value = option.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --param value '{option}'. Key cannot be empty.")
        params[key] = value
    return params


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    api_key = args.api_key or os.getenv("DOL_API_KEY")
    if not api_key:
        print("error: An API key is required (use --api-key or set DOL_API_KEY).", file=sys.stderr)
        return 1

    try:
        filter_object = parse_filter(args)
        extra_params = parse_extra_params(args.param)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    client = MshaClient(
        api_key=api_key,
        throttle_seconds=args.throttle,
        user_agent=args.user_agent,
    )

    if args.list_endpoints:
        entries = client.list_endpoints(agency=args.agency)
        if not entries:
            print(f"No endpoints found for agency '{args.agency}'.")
            return 0
        for row in entries:
            description = (row.get("description") or "").strip().replace("\n", " ")
            print(f"{row.get('agency')} / {row.get('endpoint')}: {description}")
        return 0

    if not args.endpoints:
        print("error: At least one --endpoint must be provided (see --list-endpoints).", file=sys.stderr)
        return 1

    for endpoint in args.endpoints:
        resolved_destination = client.resolve_destination(args.dest)
        print(
            f"Downloading endpoint '{endpoint}' (agency={args.agency}) "
            f"into {resolved_destination} ..."
        )
        workflow = download_msha_dataset(
            client,
            args.agency,
            endpoint,
            destination=args.dest,
            limit=args.limit,
            offset=args.offset,
            chunk_size=args.chunk_size,
            include_metadata=not args.no_metadata,
            overwrite=args.overwrite,
            filter_object=filter_object,
            extra_params=extra_params,
        )
        if not workflow.saved_paths:
            print(f"No data returned for endpoint '{endpoint}'.")
            continue

        for path in workflow.saved_paths:
            print(path)
        print(
            f"Saved {workflow.count} chunk(s) for endpoint '{endpoint}' "
            f"at {workflow.details.get('destination')}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
