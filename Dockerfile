FROM python:3.10

# install pipenv
RUN mkdir -p /kb/tmp && \
    pip install --upgrade pip && \
    pip install pipenv

# install deps
COPY Pipfile* /kb/tmp/
RUN cd /kb/tmp && \
    pipenv sync --system

# TODO remove, just here to check install is working
RUN pip freeze

