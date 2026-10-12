.PHONY: help install test lint fmt compile secrets ci clean

help:
	@echo "install      install the package plus dev tooling"
	@echo "compile      syntax-check every module (no Spark or JVM needed)"
	@echo "lint         ruff check + format check"
	@echo "fmt          ruff format (writes)"
	@echo "test         pytest against a live in-process Iceberg catalog"
	@echo "secrets      reject credential-shaped tracked files"
	@echo "ci           everything CI runs, in CI's order"

install:
	pip install -e ".[dev]"

compile:
	python -m compileall -q src tests

lint:
	ruff check src tests
	ruff format --check --diff src tests

fmt:
	ruff format src tests

test:
	pytest -v

secrets:
	@matches=$$(git ls-files | grep -iE '\.(jks|pfx|p12|keytab|pem|key|crt|cer|der)$$' || true); \
	if [ -n "$$matches" ]; then \
		echo "Credential-shaped files are tracked:"; echo "$$matches"; exit 1; \
	fi; \
	echo "no credential-shaped files tracked"

ci: compile lint test secrets

clean:
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
