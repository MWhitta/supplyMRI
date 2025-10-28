# supplyMRI EDGAR Tools

Utilities for sourcing mining-related disclosures from public repositories like the SEC's EDGAR system.

## Quick start

```bash
# Install dependencies if requests is not available
# python -m pip install requests

# Search for S-K 1300 technical report summaries and download the first 5 results
PYTHONPATH=src python scripts/download_edgar_filings.py \
  --query "S-K 1300" \
  --description-filter "Technical Report" \
  --limit 5 \
  --dest data/edgar_reports \
  --user-agent "supplyMRI data crawler (contact-you@example.com)"
```

The downloader saves each filing under `data/edgar_reports/<CIK>/<accession>/` along with a `*.metadata.json` file containing the structured response returned by the EDGAR full-text search API.

## Configuration tips

- **User-Agent**: The SEC requires a descriptive user-agent with contact information. Override the default string via `--user-agent`.
- **Rate limiting**: Requests are throttled to one every 0.3 seconds by default. Increase the delay with `--throttle` if you encounter rate-limit responses.
- **Filtering**: Scope to specific form types with `--forms` (e.g. `--forms EX-96 10-K`) or tighten the description match with `--description-filter`.
- **Paging**: Use `--start` to move deeper into the result set in 100-document increments.

## MSHA Mine Data Retrieval

The Mine Data Retrieval System (MDRS) is exposed through the Department of Labor's Open Data API and requires an API key from [https://api.dol.gov/](https://api.dol.gov/).

```bash
export DOL_API_KEY="your-key-here"

PYTHONPATH=src python scripts/download_msha_mdrs.py \
  --endpoint accident \
  --limit 200 \
  --chunk-size 100 \
  --dest data/msha_samples
```

By default results are saved under `data/msha/<agency>/<endpoint>/` with one JSON payload per chunk and matching `*.metadata.json` files summarising the request parameters. Use `--list-endpoints` to discover available MDRS datasets and `--filter-json`/`--filter-file` to pass a `filter_object` payload directly to the API.
