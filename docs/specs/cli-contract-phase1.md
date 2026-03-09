# CLI Contract (Phase 1)

Command prefix: `papersearch`

## Design Rules
- Machine-readable output only on `stdout` when `--json` is used
- Diagnostics/logs/errors on `stderr`
- Stable field names for automation
- Explicit exit codes

## Exit Codes
- `0` success
- `2` invalid input / validation error
- `3` not found
- `4` conflict
- `5` internal error

## Commands

### 1) Start Search
```bash
papersearch search "query text" --json
```
Output:
```json
{
  "search_id": "srch_...",
  "status": "queued",
  "accepted_at": "..."
}
```

### 2) Search Status
```bash
papersearch search-status <search_id> --json
```
Output:
```json
{
  "search_id": "srch_...",
  "status": "running",
  "progress": { "papers_scanned": 20, "relevant_found": 3 },
  "completeness": { "estimate": 0.31, "method": "discovery_curve_v1" }
}
```

### 3) Search Results
```bash
papersearch search-results <search_id> --limit 20 --json
```
Output:
```json
{
  "search_id": "srch_...",
  "items": [
    {
      "paper_id": "arxiv:...",
      "title": "...",
      "score": 0.92,
      "relevance": "highly_relevant"
    }
  ],
  "next_cursor": null
}
```

### 4) Create Collection
```bash
papersearch collection create "My Topic" --json
```

### 5) Add Paper to Collection
```bash
papersearch collection add <collection_id> <paper_id> --json
```

## Error Output Format (`--json-errors` optional)

If `--json-errors` is on, write errors to `stderr` as JSON lines:
```json
{"code":"INVALID_ARGUMENT","message":"search_id required","details":{}}
```

Default stderr is human-readable.

## Stability Policy
- Treat `--json` as a public API surface
- Backward-incompatible output changes require MAJOR bump
- Additive fields are MINOR
- Bugfix-only behavior changes are PATCH
