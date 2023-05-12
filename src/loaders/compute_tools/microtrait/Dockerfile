FROM mambaorg/micromamba:1.4.1

# Note from micromamba - using an environment name other than "base" is not recommended
ENV CONDA_ENV base
# Set the value of the environment variable ENV_TOOL to micromamba, which is used by the compute_genome_entrypoint.sh script
ENV ENV_TOOL micromamba

# The user and group IDs, both denoted as "$MAMBA_USER," are defined within the base image
# use --chown=$MAMBA_USER:$MAMBA_USER to set the owner of the files to the user $MAMBA_USER is recommended
# https://github.com/mamba-org/micromamba-docker#quick-start
COPY --chown=$MAMBA_USER:$MAMBA_USER /src/loaders/compute_tools/microtrait/env_microtrait.yaml /tmp/env_microtrait.yaml
RUN micromamba install -y -n $CONDA_ENV -f /tmp/env_microtrait.yaml && \
    micromamba clean --all --yes

ENV PATH=$PATH:/opt/conda/bin/

# Install microTrait dependencies
RUN Rscript -e 'install.packages("kmed", version = "0.4.2", repos = "https://cloud.r-project.org/")' && \
    Rscript -e 'devtools::install_github("jlw-ecoevo/gRodon")'

# Install microTrait
ARG MT_RELEASE_TAG=kb
RUN Rscript -e "devtools::install_github('ukaraoz/microtrait', ref = '${MT_RELEASE_TAG}')"

# Setup of HMM database
RUN Rscript -e 'library(microtrait)' && \
    Rscript -e 'microtrait::prep.hmmmodels()' && \
    Rscript -e 'list.files(file.path(.libPaths(), "microtrait/extdata/hmm/hmmpress"))'

COPY --chown=$MAMBA_USER:$MAMBA_USER ./ /app/collections
RUN rm -r /app/collections/.git
ENV PYTHONPATH /app/collections

WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/microtrait/microtrait.py

RUN chmod -R 777 /app/collections

# Download microtrait repository
ARG MT_REPO_URL=https://github.com/ukaraoz/microtrait
ARG MT_DOWNLOAD_URL=${MT_REPO_URL}/archive/refs/tags/${MT_RELEASE_TAG}.zip
RUN curl -L ${MT_DOWNLOAD_URL} -o release.zip \
    && unzip release.zip \
    && rm release.zip

# Generate trait to unwrapped rules mapping file
ARG MT_RULE2TRAIT_FILE=/app/microtrait-${MT_RELEASE_TAG}/data-raw/microtrait_rule2trait.txt
ARG MT_RULEUNWRAPPED_FILE=/app/microtrait-${MT_RELEASE_TAG}/data-raw/microtrait_ruleunwrapped.txt
ENV MT_TRAIT_UNWRAPPED_FILE=/app/microtrait_trait2unwrappedrules.txt

RUN python -c "\
from src.loaders.compute_tools.microtrait.microtrait_util import create_trait_unwrapped_rules; \
create_trait_unwrapped_rules('${MT_RULE2TRAIT_FILE}', '${MT_RULEUNWRAPPED_FILE}', '${MT_TRAIT_UNWRAPPED_FILE}')"

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]
