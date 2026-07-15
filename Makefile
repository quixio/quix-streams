.PHONY: help lint format format-check typecheck check

.DEFAULT_GOAL := help

# Pick the first Python that actually runs (a venv symlink may exist but be
# broken/platform-mismatched). Invoking tools via `python -m` also avoids
# platform-mismatched console binaries. Override with `make lint PYTHON=...`.
# Order: activated venv -> ./.venv -> python3 -> python.
PYTHON ?= $(shell for p in "$$VIRTUAL_ENV/bin/python" .venv/bin/python python3 python; do \
	"$$p" -c '' >/dev/null 2>&1 && echo "$$p" && break; done)
RUFF ?= $(PYTHON) -m ruff
MYPY ?= $(PYTHON) -m mypy

# Source dirs mypy is configured to check (see .pre-commit-config.yaml)
MYPY_PATHS := quixstreams

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

format: ## Auto-fix lint issues and reformat code in place
	$(RUFF) check --fix .
	$(RUFF) format .

lint: format-check typecheck ## Lint + format check + type check, no modifications (CI-friendly)
	$(RUFF) check .

format-check: ## Verify formatting without changing files
	$(RUFF) format --check .

typecheck: ## Run static type checking (mypy)
	$(MYPY) $(MYPY_PATHS)

check: lint ## Run everything (alias for lint)
