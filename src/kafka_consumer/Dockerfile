FROM python:3.11-slim

RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH to include src directory
ENV PYTHONPATH=/app

CMD ["python", "src/kafka_consumer/consumer.py"]
