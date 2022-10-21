FROM python:3.10

RUN mkdir -p /kb/collections
WORKDIR /kb/collections

# install pipenv
RUN pip install --upgrade pip && \
    pip install pipenv

# install deps
COPY Pipfile* ./
RUN pipenv sync --system

COPY ./ /kb/collections/

# Write the git commit for the service
ARG VCS_REF=no_git_commit_passed_to_build
RUN echo "GIT_COMMIT=\"$VCS_REF\"" > src/common/git_commit.py

# FastAPI recommends running a single process service per docker container instance as below,
# and scaling via adding more containers. If we need to run multiple processes, use guvicorn as
# a process manger as described in the FastAPI docs
ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "5000", "--factory", "src.service.app:create_app"]

