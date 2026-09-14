# Aggregator Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./aggregator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./aggregator/ .

# Create data directory
RUN mkdir -p /app/data

# Run the aggregator
CMD ["python", "main.py"]