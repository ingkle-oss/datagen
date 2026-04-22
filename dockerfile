FROM ghcr.io/astral-sh/uv:0.8.19 AS uv
FROM python:3.11

ENV APPNAME=datagen
ENV UV_SYSTEM_PYTHON=1

COPY --from=uv /uv /uvx /bin/
WORKDIR /opt/${APPNAME}/

COPY pyproject.toml uv.lock /opt/${APPNAME}/
RUN uv sync --frozen --no-dev

COPY src /opt/${APPNAME}/

ENV PATH="/opt/${APPNAME}/.venv/bin:$PATH"
CMD ["python3"]
