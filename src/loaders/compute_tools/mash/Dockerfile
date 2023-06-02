FROM continuumio/miniconda3:22.11.1

RUN apt-get update && \
    apt-get install -y wget

# Set the Mash version argument
# using version 2.0 to maintain compatibility with the homology service
ARG MASH_VER=2.0
ENV CONDA_ENV mash-$MASH_VER

# Download Mash
RUN wget https://github.com/marbl/Mash/releases/download/v${MASH_VER}/mash-Linux64-v${MASH_VER}.tar && \
    tar -xf mash-Linux64-v${MASH_VER}.tar && \
    mv mash-Linux64-v${MASH_VER}/mash /usr/local/bin && \
    rm -rf mash-Linux64-v${MASH_VER}.tar mash-Linux64-v${MASH_VER}

# Set up the Conda environment
ENV PATH="/opt/conda/bin:${PATH}"
ARG PYTHON_VER=3.11
RUN conda config --add channels conda-forge
RUN conda create -n $CONDA_ENV python=$PYTHON_VER
RUN conda install -n $CONDA_ENV pandas=2.0.2
RUN echo "source activate $CONDA_ENV" >> ~/.bashrc

RUN mkdir -p /app
COPY ./ /app/collections
# slows down that chmod step if left in place
RUN rm -r /app/collections/.git

ENV PYTHONPATH /app/collections

WORKDIR /app

ENV PY_SCRIPT=/app/collections/src/loaders/compute_tools/mash/mash.py

RUN chmod -R 777 /app/collections

ENTRYPOINT ["/app/collections/src/loaders/compute_tools/entrypoint.sh"]