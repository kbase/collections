FROM continuumio/miniconda3:22.11.1

ENV GTDB_VER 2.3.2
ENV CONDA_ENV gtdbtk-$GTDB_VER
ENV PYTHON_VER 3.8

RUN conda config --add channels bioconda
RUN conda config --add channels conda-forge

RUN conda create -n $CONDA_ENV python=$PYTHON_VER
RUN conda install -n $CONDA_ENV -c conda-forge -c bioconda gtdbtk=$GTDB_VER
RUN conda install -n $CONDA_ENV numpy=1.23.1 jsonlines=3.1.0 pandas=2.0.3

RUN echo "source activate $CONDA_ENV" >> ~/.bashrc

RUN mkdir -p /reference_data
RUN mkdir -p /app
COPY ./ /app/collections
RUN rm -r /app/collections/.git

ENV GTDBTK_DATA_PATH /reference_data
ENV PYTHONPATH /app/collections

WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/gtdb_tk/gtdb_tk.py

RUN chmod -R 777 /app/collections

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]
