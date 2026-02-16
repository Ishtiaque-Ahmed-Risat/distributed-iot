# Batch Analytics & ML Training Job with PySpark
FROM python:3.11-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# This container exits after the job completes
CMD ["python", "-u", "main.py"]
