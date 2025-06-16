FROM python:3.10-slim-bullseye

WORKDIR /app
COPY . .
RUN apt-get update && apt-get upgrade -y && apt-get dist-upgrade -y && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt
CMD ["python", "kafka_to_supabase_etl.py"]
