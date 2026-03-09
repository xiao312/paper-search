# paper-search-agent (Phase 1)

Local/private-first paper search + ingest pipeline (CLI + MCP + outbound Feishu webhook).

Local/private-first implementation:
- Core search/organize logic
- CLI (`papersearch`)
- MCP server (`papersearch-mcp`) over stdio
- Feishu outbound webhook notifier (optional)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## CLI examples

```bash
papersearch search "retrieval augmented generation benchmarks" --json
papersearch search-status srch_xxx --json
papersearch search-results srch_xxx --limit 10 --json
papersearch collection create "RAG reading list" --json
papersearch collection add col_xxx mock:1234.0001 --json
papersearch discover "multimodal retrieval" --limit 5 --mock --json
papersearch ingest-doi 10.1016/j.mock.2024.123456 --mock --json
# live: try Elsevier XML + download figure assets when available
papersearch ingest-doi 10.1016/j.egyai.2024.100341 --json
```

## MCP quick check

```bash
printf '{"jsonrpc":"2.0","id":1,"method":"tools/list"}\n' | papersearch-mcp
```

## Optional Feishu outbound notifications

Set env vars:

```bash
export FEISHU_NOTIFY_ENABLED=true
export FEISHU_WEBHOOK_URL='https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
# export FEISHU_SIGNING_SECRET='...'
```

Then `papersearch search ...` will send completion notification.

`ingest-doi` downloads figure assets by default when URLs are available.
Use `--no-assets` to skip image downloads.

## Development

Mock tests:
```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'
```

Quality checks (optional dev extras):
```bash
pip install -e '.[dev]'
ruff check src tests
mypy src
```

Live smoke (needs internet; Elsevier full text may require key + entitlement/IP):
```bash
PYTHONPATH=src python3 tests/live_smoke.py
```

## GitHub

- CI: `.github/workflows/ci.yml`
- Contribution guide: `CONTRIBUTING.md`
- License: `LICENSE` (MIT)

Before committing, copy `.env.example` to `.env` and keep secrets out of git.
