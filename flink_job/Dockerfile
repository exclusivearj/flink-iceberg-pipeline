FROM flink:1.18-scala_2.12-java11

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install Python 3.11 + pip, plus the toolchain needed to build apache-flink's
# transitive deps from source on arm64 (no wheels published): g++ for pemja's C
# extension, and full JDK headers for its JNI build — the base image ships only
# a JRE, so /opt/java/openjdk/include (jni.h) is missing.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        g++ openjdk-11-jdk-headless \
        curl ca-certificates && \
    ln -sf /usr/bin/python3 /usr/local/bin/python && \
    ln -sf "$(dirname "$(find /usr/lib/jvm -name jni.h | head -1)")" /opt/java/openjdk/include && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Pull connector JARs into /opt/flink/lib
# iceberg-flink-runtime-1.18 was first published at 1.5.0 (the 1.4.x line has no
# Flink 1.18 runtime), so 1.5.2 is the lowest version with both required artifacts.
ARG FLINK_KAFKA_VER=3.1.0-1.18
ARG ICEBERG_VER=1.5.2
# Flink's Iceberg connector instantiates a Hadoop Configuration during catalog
# init (even for a REST catalog + S3FileIO), but the flink:1.18 base ships no
# Hadoop classes — without this uber jar, CREATE CATALOG throws
# ClassNotFoundException: org.apache.hadoop.conf.Configuration.
ARG HADOOP_UBER_VER=2.8.3-10.0
RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VER}/flink-sql-connector-kafka-${FLINK_KAFKA_VER}.jar; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VER}/iceberg-flink-runtime-1.18-${ICEBERG_VER}.jar; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VER}/iceberg-aws-bundle-${ICEBERG_VER}.jar; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VER}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VER}.jar

# Filesystem checkpoint storage dir (shared via a named volume across JM/TM in
# compose). The default JobManager in-memory storage caps state at 5 MB, which
# the Kafka source + window state exceeds — see docker-compose state.checkpoints.dir.
# World-writable so it works regardless of the runtime uid.
RUN mkdir -p /opt/flink/checkpoints && chmod 777 /opt/flink/checkpoints

# Copy job code. The modules use absolute package imports (`from flink_job.x
# import ...`), so the package PARENT must be on the path, not the package dir.
COPY . /opt/flink_job/
ENV PYTHONPATH=/opt
