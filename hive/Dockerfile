# Hive Metastore with S3A (MinIO) support
FROM apache/hive:4.0.0

USER root

# Install wget
RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

# Download required JARs for S3A filesystem and PostgreSQL
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -O /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -O /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
        -O /opt/hive/lib/postgresql-42.7.4.jar

# Set permissions
RUN chmod 644 /opt/hive/lib/hadoop-aws-3.3.4.jar \
              /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
              /opt/hive/lib/postgresql-42.7.4.jar

USER hive
