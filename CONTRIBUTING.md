# Contributing

Use small pull requests, include tests for behavior changes, and keep connector configuration changes documented in `docs/operations.md`.

Before opening a PR:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
make lint
make test
```

For infrastructure changes, also run `make reset && make demo` locally with Docker Desktop or Docker Engine.
