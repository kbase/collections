FROM continuumio/miniconda3:24.1.2-0

ENV EGGNOG_VER 2.1.12
ENV CONDA_ENV eggnog-$EGGNOG_VER
ENV PYTHON_VER 3.11

RUN conda config --add channels bioconda
RUN conda config --add channels conda-forge

RUN conda create -n $CONDA_ENV python=$PYTHON_VER
RUN conda install -n $CONDA_ENV -c conda-forge -c bioconda eggnog-mapper=$EGGNOG_VER
RUN conda install -n $CONDA_ENV pandas=2.2.1 jsonlines=2.0.0

RUN echo "source activate $CONDA_ENV" >> ~/.bashrc

# eggNOG annotation DB is pre-downloaded at /global/cfs/cdirs/kbase/collections/libraries/eggnog/5.0.2
# following instructions at https://github.com/eggnogdb/eggnog-mapper/wiki/eggNOG-mapper-v2.1.5-to-v2.1.12#setup
# Mount the annotation DB directory to /reference_data when running the container
ENV EGGNOG_DATA_DIR /reference_data

RUN mkdir -p /app
COPY ./ /app/collections
# slows down that chmod step if left in place
RUN rm -r /app/collections/.git

ENV PYTHONPATH /app/collections

WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/eggnog/eggnog.py

RUN chmod -R 777 /app/collections

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]