# Batch Analytics & ML Training Job
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# This container exits after the job completes
CMD ["python", "-u", "main.py"]
