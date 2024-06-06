.PHONY: tests
tests:
		pytest tests/

.PHONY: format
format:
	black src/ tests/ scripts/
