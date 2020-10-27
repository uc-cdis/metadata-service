FROM python:3.7-alpine as base

FROM base as builder
RUN apk add --no-cache --virtual .build-deps gcc musl-dev libffi-dev openssl-dev make postgresql-dev git curl
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
COPY . /src/
WORKDIR /src
RUN python -m venv /env && . /env/bin/activate && $HOME/.poetry/bin/poetry install --no-dev --no-interaction

FROM base
RUN apk add --no-cache postgresql-libs
COPY --from=builder /env /env
COPY --from=builder /src /src
WORKDIR /src
CMD ["/env/bin/gunicorn", "mds.asgi:app", "-b", "0.0.0.0:80", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn.conf.py"]
