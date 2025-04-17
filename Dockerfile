# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:debian-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=.python-version,target=.python-version \
    uv sync --frozen --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:/root/.duckdb/cli/latest:$PATH"

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates ledger curl
RUN curl https://install.duckdb.org | sh
ENV HOME="/app"
CMD [ "/app/code/accounts/app.py" ]
EXPOSE 8080/tcp