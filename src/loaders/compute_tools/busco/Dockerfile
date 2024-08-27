FROM continuumio/miniconda3:24.5.0-0

# NOTE: If the tool version changes, ensure the metadata information saved after running the tool in the _run_busco_single method is updated
ARG BUSCO_VER=5.7.1
ENV CONDA_ENV busco-$BUSCO_VER

# Add Bioconda and Conda-Forge channels
RUN conda config --add channels bioconda
RUN conda config --add channels conda-forge

# Install BUSCO
# Certain dependencies (e.g., dendropy, sepp) are only compatible with Python versions up to 3.9.
ARG PYTHON_VER=3.9
RUN conda create -n $CONDA_ENV python=$PYTHON_VER
RUN conda install -n $CONDA_ENV pandas=2.2.2 jsonlines=4.0.0 mamba=1.5.8 pyyaml=6.0.1
# Suggestions from BUSCO team to use mamba for speeding up the installation process:
# https://busco.ezlab.org/busco_userguide.html#installation-with-conda
RUN conda run -n $CONDA_ENV mamba install -c bioconda -c conda-forge -y busco=$BUSCO_VER

# Activate the environment
RUN echo "source activate $CONDA_ENV" >> ~/.bashrc

# Set up directories
RUN mkdir -p /app
COPY ./ /app/collections
RUN rm -r /app/collections/.git

ENV PYTHONPATH /app/collections
WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/busco/busco.py

RUN chmod -R 777 /app/collections

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]
