# One image for the API and every worker; Compose selects the entry command.
FROM python:3.12.16-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies first so source edits reuse the cached layer.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --locked --no-dev --no-install-project

COPY backend ./backend
RUN uv sync --locked --no-dev

COPY alembic.ini ./
COPY migrations ./migrations

RUN useradd --system --uid 10001 --home-dir /app signalops
USER signalops

ENV PATH="/opt/venv/bin:${PATH}"

ENTRYPOINT ["signalops"]
CMD ["--help"]
