# Imagen base ligera de Python
FROM python:3.11-slim

# Establecer directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias del sistema necesarias para MySQL Connector
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copiar dependencias de Python
COPY requirements.txt requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY . .

# Exponer el puerto que usará tu servicio (Cloud Run ignora este valor, pero es buena práctica)
EXPOSE 8080

# Comando de inicio (ajusta a tu archivo principal, por ejemplo main.py o app.py)
CMD ["python", "app.py"]
