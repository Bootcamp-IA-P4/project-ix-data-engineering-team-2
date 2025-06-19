FROM python:3.10-slim

# Instalar netcat para wait-for-it.sh
#RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Instalar netcat para wait-for-it.sh
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

WORKDIR /app



# Copy all files
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH to include src directory
#ENV PYTHONPATH=/app

# Make wait-for-it.sh executable
#RUN chmod +x /app/src/kafka_consumer/wait-for-it.sh



# Copy consumer files
COPY *.py .

# The src directory will be mounted via docker-compose volumes
# So we don't need to copy the entire src structure here




# Set working directory to where the code will be mounted
 WORKDIR /app/src/kafka_consumer


# Use wait-for-it.sh to wait for Kafka
# CMD ["python", "consumer.py"]

CMD ["sh", "-c", "while ! nc -z kafka 9092; do echo 'Esperando Kafka...'; sleep 2; done; echo 'Kafka disponible!'; python consumer.py"]
