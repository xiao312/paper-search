# Contributing

Thanks for contributing.

## Development setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

## Before opening a PR

Run:

```bash
ruff check src tests
mypy src
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'
```

## Scope and style

- Keep changes minimal and focused.
- Preserve local/private-first architecture (no public inbound endpoints in phase 1).
- Prefer readability over abstraction.
- Add tests for behavior changes, especially ingestion parsing/rendering.
