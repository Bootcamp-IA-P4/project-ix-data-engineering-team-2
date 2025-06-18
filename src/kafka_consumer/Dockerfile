FROM python:3.10-slim

# Instalar netcat para wait-for-it.sh
#RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH to include src directory
#ENV PYTHONPATH=/app

# Make wait-for-it.sh executable
#RUN chmod +x /app/src/kafka_consumer/wait-for-it.sh

# Use wait-for-it.sh to wait for Kafka
CMD ["python", "consumer.py"]
