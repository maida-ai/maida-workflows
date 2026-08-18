.PHONY: docs docs-clean docs-serve test

# Build the documentation. Warnings are errors so broken references fail CI.
docs:
	uv run --group docs sphinx-build -W --keep-going -E \
		-b dirhtml -d .sphinx-doctrees docs site

docs-clean:
	rm -rf site .sphinx-doctrees

docs-serve: docs
	python3 -m http.server --directory site 8000

test:
	uv run pytest
