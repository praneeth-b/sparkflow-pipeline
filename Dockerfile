# =============================================================================
# Airflow + PySpark image  (ARM64 / Apple Silicon)
# =============================================================================
FROM apache/airflow:2.10.4

# ── Root: system packages & runtime directories ───────────────────────────────
USER root

# Install OpenJDK 17 (ARM64-native) and procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Create Spark runtime dirs as root so all child JVM processes (worker,
# executor) can write here regardless of UID.
RUN mkdir -p /tmp/spark-work /tmp/spark-local && \
    chmod 777 /tmp/spark-work /tmp/spark-local

# ── Airflow user: Python packages ─────────────────────────────────────────────
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ── CORE FIX: satisfy Spark's AbstractCommandBuilder.getScalaVersion() ────────
#
# When the Spark worker launches an executor JVM it invokes
# AbstractCommandBuilder.buildClassPath() → getScalaVersion(), which scans
# SPARK_HOME for one of these two directories:
#
#   $SPARK_HOME/launcher/target/scala-2.12
#   $SPARK_HOME/launcher/target/scala-2.13
#
# A pip-installed PySpark has neither, so every executor immediately dies with:
#   java.lang.IllegalStateException: Cannot find any build directories.
#
# Solution A (directory stub) — create the exact path the scanner expects:
RUN mkdir -p \
      /home/airflow/.local/lib/python3.12/site-packages/pyspark/launcher/target/scala-2.12

# Solution B (env var bypass) — SPARK_SCALA_VERSION is set in docker-compose so
# getScalaVersion() returns immediately without scanning. Belt-and-suspenders.

# ── spark-defaults.conf ───────────────────────────────────────────────────────
# Child executor JVMs do not inherit shell env vars, so critical Spark settings
# must be baked into spark-defaults.conf to be guaranteed available everywhere.
RUN mkdir -p \
      /home/airflow/.local/lib/python3.12/site-packages/pyspark/conf && \
    printf '%s\n' \
      'spark.local.dir                 /tmp/spark-local' \
      'spark.worker.dir                /tmp/spark-work' \
      'spark.executor.extraJavaOptions  -Djava.net.preferIPv4Stack=true' \
      'spark.driver.extraJavaOptions    -Djava.net.preferIPv4Stack=true' \
    > /home/airflow/.local/lib/python3.12/site-packages/pyspark/conf/spark-defaults.conf