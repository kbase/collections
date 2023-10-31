FROM continuumio/miniconda3:22.11.1

ARG CHECKM2_VER=1.0.1
ENV CONDA_ENV checkm2-$CHECKM2_VER

RUN conda config --add channels bioconda
RUN conda config --add channels conda-forge

# install CheckM2
ARG PYTHON_VER=3.8
RUN conda create -n $CONDA_ENV python=$PYTHON_VER checkm2=$CHECKM2_VER
RUN conda install -n $CONDA_ENV pandas=1.4.0 jsonlines=3.1.0

RUN echo "source activate $CONDA_ENV" >> ~/.bashrc

RUN mkdir -p /reference_data
RUN mkdir -p /app
COPY ./ /app/collections
# slows down that chmod step if left in place
RUN rm -r /app/collections/.git

# Set pre-downloaded Diamond database. Mount the download directory to /reference_data when running the container.
# Confirm that the downloaded Diamond database name matches the one specified in the Dockerfile.
# If not, please update the DIAMOND_DB_NAME accordingly.
# See download DB instructions at: https://github.com/chklovski/CheckM2#database
ARG DIAMOND_DB_NAME=uniref100.KO.1.dmnd
ENV CHECKM2DB /reference_data/$DIAMOND_DB_NAME

ENV PYTHONPATH /app/collections

WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/checkm2/checkm2.py

RUN chmod -R 777 /app/collections

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]
