# Producer Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./producer/ .
COPY ./config /app/config

# Create data directory
RUN mkdir -p /app/data

# Run the producer
CMD ["python", "main.py"]