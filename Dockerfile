FROM python:3.7
RUN pip install poetry==1.0.0
COPY . /src/
WORKDIR /src
RUN poetry install
CMD ["/usr/local/bin/poetry", "run", "gunicorn", "mds.app:app", "-b", "0.0.0.0:80", "-k", "uvicorn.workers.UvicornWorker"]
