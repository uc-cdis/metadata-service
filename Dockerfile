FROM python:3.7
RUN pip install poetry
COPY . /src/
WORKDIR /src
RUN poetry install
CMD ["/usr/local/bin/poetry", "run", "gunicorn", "mds.app:app", "-k", "uvicorn.workers.UvicornWorker"]
