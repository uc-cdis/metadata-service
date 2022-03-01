FROM quay.io/cdis/python:python3.10-buster-pybase3-3.0.2 as base

FROM base as builder
RUN pip install --upgrade pip poetry
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential gcc make musl-dev libffi-dev libssl-dev git curl
COPY . /src/
WORKDIR /src
RUN python -m venv /env && . /env/bin/activate && poetry install -vv --no-interaction

FROM base
RUN apt-get install curl
COPY --from=builder /env /env
COPY --from=builder /src /src
ENV PATH="/env/bin/:${PATH}"
WORKDIR /src
CMD ["/env/bin/gunicorn", "mds.asgi:app", "-b", "0.0.0.0:80", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn.conf.py"]
