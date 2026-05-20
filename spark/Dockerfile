FROM apache/spark:3.5.1 AS spark-base

FROM python:3.11-slim-bookworm

# Install default-jre-headless, procps (needed for Spark), and curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copy Spark from the official image
COPY --from=spark-base /opt/spark /opt/spark

# Create spark user and group (using original UID/GID 185)
RUN groupadd -g 185 spark && \
    useradd -u 185 -g spark -d /opt/spark/work-dir -s /bin/bash spark && \
    chown -R spark:spark /opt/spark

WORKDIR /opt/spark/work-dir
USER spark
