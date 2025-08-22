# Imagen base oficial de Python 3.11 slim
FROM python:3.11-slim

# Evitar mensajes interactivos y locales
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements e instalar dependencias
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar el resto de la app
COPY . .

# Puerto que Cloud Run expondrá
ENV PORT 8080

# Comando para correr la app
CMD ["python", "app.py"]
