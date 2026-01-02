FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y tree && \
    pip install "pyspark[pipelines]"

WORKDIR /app