FROM flink:1.18-scala_2.12-java11

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install Python 3.11 + pip
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        curl ca-certificates && \
    ln -sf /usr/bin/python3 /usr/local/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Pull connector JARs into /opt/flink/lib
ARG FLINK_KAFKA_VER=3.1.0-1.18
ARG ICEBERG_VER=1.4.2
RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VER}/flink-sql-connector-kafka-${FLINK_KAFKA_VER}.jar; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VER}/iceberg-flink-runtime-1.18-${ICEBERG_VER}.jar; \
    curl -fsSL -O https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VER}/iceberg-aws-bundle-${ICEBERG_VER}.jar

# Copy job code
COPY . /opt/flink_job/
ENV PYTHONPATH=/opt/flink_job
