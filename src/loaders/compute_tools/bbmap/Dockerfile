FROM continuumio/miniconda3:24.5.0-0

# NOTE: If the tool version changes, ensure the metadata information saved after running the tool in the _run_bbmap_single method is updated
ARG BBMAP_VER=39.06
ENV CONDA_ENV bbmap-$BBMAP_VER

# Add Bioconda and Conda-Forge channels
RUN conda config --add channels bioconda
RUN conda config --add channels conda-forge

# Install BBMap
ARG PYTHON_VER=3.11
RUN conda create -n $CONDA_ENV python=$PYTHON_VER bbmap=$BBMAP_VER
RUN conda install -n $CONDA_ENV pandas=2.2.2 jsonlines=4.0.0

# Activate the environment
RUN echo "source activate $CONDA_ENV" >> ~/.bashrc

# Set up directories
RUN mkdir -p /app
COPY ./ /app/collections
RUN rm -r /app/collections/.git


ENV PYTHONPATH /app/collections
WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/bbmap/bbmap.py

RUN chmod -R 777 /app/collections

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]
