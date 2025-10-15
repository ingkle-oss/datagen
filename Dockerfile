FROM python:3.11

ARG APPNAME=datagen

COPY --from=ghcr.io/astral-sh/uv:0.8.11 /uv /uvx /bin/

ADD ./pyproject.toml /app/${APPNAME}/pyproject.toml
ADD ./uv.lock /app/${APPNAME}/uv.lock
ADD ./src /app/${APPNAME}/src
ADD ./samples /app/samples

WORKDIR /app/${APPNAME}
RUN uv sync --locked

CMD ["uv", "run"]
