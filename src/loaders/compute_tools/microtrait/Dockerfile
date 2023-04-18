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

# Install microTrait
RUN Rscript -e 'install.packages("kmed", repos = "https://cloud.r-project.org/")' && \
    Rscript -e 'devtools::install_github("jlw-ecoevo/gRodon")' && \
    Rscript -e 'devtools::install_github("ukaraoz/microtrait")'

# Setup of HMM database
RUN Rscript -e 'library(microtrait)' && \
    Rscript -e 'microtrait::prep.hmmmodels()' && \
    Rscript -e 'list.files(file.path(.libPaths(), "microtrait/extdata/hmm/hmmpress"))'

COPY --chown=$MAMBA_USER:$MAMBA_USER ./ /app/collections
ENV PYTHONPATH /app/collections

WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/microtrait/microtrait.py
ENV ENTRYPOINT_PATH /app/collections/src/loaders/compute_tools/entrypoint.sh

RUN chmod -R 777 /app/collections

ENTRYPOINT [$ENTRYPOINT_PATH]
