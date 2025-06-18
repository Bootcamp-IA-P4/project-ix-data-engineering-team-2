FROM python:3.10-slim

# Instalar netcat para wait-for-it.sh
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar los archivos del proyecto
COPY . .

# Instalar dependencias
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Configurar PYTHONPATH
ENV PYTHONPATH=/app

# Hacer el script ejecutable
#RUN chmod +x /app/src/etl/wait-for-it.sh

# Ejecutar el script
CMD ["python", "mongo_to_postgres.py"]