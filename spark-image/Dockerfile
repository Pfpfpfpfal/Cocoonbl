FROM spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

ENV PATH="opt/spark/bin:${PATH}"
ENV HOME=/root

RUN mkdir -p ${HOME}

RUN pip install --no-cache-dir jupyterlab ipykernel pyspark==3.5.1 \
    && python3 -m ipykernel install --name pyspark --display-name "Pyspark (container)" --sys-prefix

COPY jars/*jar /opt/spark/jars/

WORKDIR /workspace