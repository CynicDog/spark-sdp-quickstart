# spark-sdp-quickstart

Minimal setup for experimenting with **Spark Declarative Pipelines (SDP)** using Spark 4.1.

## Requirements

* Docker

## Run Spark

```bash
docker run -it \
  --name spark-sdp-test \
  --user root \
  apache/spark:4.1.0-scala2.13-java21-python3-ubuntu \
  /bin/bash
```

## Setup

```bash
apt-get update && apt-get install -y tree

mkdir -p /app
cd /app

pip install "pyspark[pipelines]"
```

## Initialize SDP Project

```bash
/opt/spark/bin/spark-pipelines init --name sdp_demo
```

## ...
```bash
root@ed568c711d10:/app/sdp# spark-pipelines run     
```

## Notes

* SDP is **CLI-driven**, not for notebooks or interactive execution
* Pipelines are defined declaratively and executed by Spark
