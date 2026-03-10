# Use the official Airflow image as the base
FROM apache/airflow:2.10.4

# Switch to root to install system dependencies (Java)
USER root

# Install OpenJDK 17 (Native and stable for Apple Silicon / ARM64) and procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Switch back to the airflow user to install Python packages
USER airflow

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# create symlinks so the JVM can find build resources
RUN mkdir -p /tmp/spark-work /tmp/spark-local && \
    chmod 777 /tmp/spark-work /tmp/spark-local

# Ensure pyspark zip files are accessible
RUN ls /home/airflow/.local/lib/python3.12/site-packages/pyspark/python/lib/