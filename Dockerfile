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

# file is created in entrypoint.sh
ENV KB_DEPLOYMENT_CONFIG=collections_config.toml

#RUN chmod a+x scripts/entrypoint.sh
ENTRYPOINT ["scripts/entrypoint.sh"]

