# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run expone el puerto en la env var $PORT
ENV PORT=8080

# Gunicorn recomendado para producción
CMD exec gunicorn --bind :$PORT --workers 2 --threads 8 --timeout 0 app:app
