ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

# FROM 707767160287.dkr.ecr.us-east-1.amazonaws.com/gen3/python-build-base:${AZLINUX_BASE_VERSION} as base

ENV appname=mds

COPY --chown=gen3:gen3 /src/${appname} /${appname}

WORKDIR /${appname}

# Builder stage
FROM base AS builder

RUN mkdir /env && \
    chown -R gen3:gen3 /env

USER gen3

COPY poetry.lock pyproject.toml /${appname}/

RUN python3 -m venv /env && . /env/bin/activate && poetry install -vv --no-interaction --without dev

COPY --chown=gen3:gen3 . /${appname}
COPY --chown=gen3:gen3 ./deployment/wsgi/wsgi.py /${appname}/wsgi.py

# Run poetry again so this app itself gets installed too
RUN python3 -m venv /env && . /env/bin/activate && poetry install -vv --no-interaction --without dev

# Final stage
FROM base

COPY --from=builder /env /env
COPY --from=builder /${appname} /${appname}

# Switch to non-root user 'gen3' for the serving process

USER gen3

RUN source /env/bin/activate

# Add /env/bin to PATH
ENV PATH="/env/bin:$PATH"

CMD ["/bin/bash", "-c", "/${appname}/dockerrun.bash"]
