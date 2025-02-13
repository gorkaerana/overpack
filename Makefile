mypy:
	uv run mypy .

pyright:
	uv run pyright .

type_check: mypy pyright

format:
	uv run ruff format

lint:
	uv run ruff check

# qc = quality control
qc: format lint type_check

install_dev:
	uv sync --all-groups

test:
	uv run pytest tests/test___init__.py --workers=auto
