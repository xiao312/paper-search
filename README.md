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
papersearch bohrium-create-session "ammonia natural gas combustion kinetics with deep learning surrogates" --sigma-model auto --discipline ET --json
papersearch bohrium-session-detail <uuid> --json
papersearch bohrium-question-papers <query_id> --sort RelevanceScore --json
papersearch llm list-models --provider openrouter --json
papersearch llm prompt "Summarize ammonia/natural-gas combustion ML trends in 5 bullets" --provider openrouter --model anthropic/claude-sonnet-4 --thinking low --json
papersearch seed-candidates "ammonia/natural gas combustion kinetics with deep learning surrogates" <query_id> --top-k 20 --provider zai --model glm-4.5-flash --thinking low --json
papersearch seed-candidates-auto "ammonia/natural gas combustion kinetics with deep learning surrogates" --top-k 20 --min-seed-count 5 --crossref-rows 30 --sigma-model auto --discipline ET --provider zai --model glm-4.5-flash --thinking low --json
papersearch relevance-classify-queryid "ammonia natural gas combustion kinetics with deep learning surrogates" <query_id> --top-k 20 --provider zai --model glm-4.5-flash --thinking off --max-workers 2 --json
papersearch op-search "ammonia natural gas combustion kinetics with deep learning surrogates" --top-k 20 --min-seed-count 5 --crossref-rows 30 --json
papersearch op-classify "ammonia natural gas combustion kinetics with deep learning surrogates" <query_id> --top-k 20 --json
papersearch op-grow "10.1016/j.fuel.2026.138904" --levels 2 --limit-per-node 30 --json
papersearch ingest-doi 10.1016/j.mock.2024.123456 --mock --json
# live: try Elsevier XML + download figure assets when available
papersearch ingest-doi 10.1016/j.egyai.2024.100341 --json

# graph (API-first, DOI-centric)
papersearch graph ingest-doi 10.1016/j.egyai.2024.100341 --json
papersearch graph stats --json
papersearch graph neighbors 10.1016/j.egyai.2024.100341 --direction out --json
papersearch graph related 10.1016/j.egyai.2024.100341 --mode coupling --json
papersearch graph prior 10.1016/j.egyai.2024.100341 --direction in --json
papersearch graph derivative 10.1016/j.egyai.2024.100341 --direction in --json
papersearch graph related-set 10.1016/j.egyai.2024.100341,10.1016/j.egyai.2021.100128 --mode coupling --json
papersearch graph ingest-openalex-journals "Fuel,Combustion and Flame,Energy,Proceedings of the Combustion Institute" --per-journal 10 --json
papersearch graph backfill-openalex-journal "Proceedings of the Combustion Institute" --json
papersearch graph expand "10.1016/j.proci.2020.06.375" --rounds 2 --max-new-per-round 100 --max-workers 2 --json
papersearch graph rank "10.1016/j.proci.2020.06.375" --limit 20 --max-iter 100 --tol 1e-7 --json
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
