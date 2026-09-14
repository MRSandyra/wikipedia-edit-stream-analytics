# Consumer Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./consumer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./consumer/ .
COPY ./config /app/config

# Create data directory
RUN mkdir -p /app/data/bronze

# Run the consumer
CMD ["python", "main.py"]