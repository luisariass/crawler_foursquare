FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

WORKDIR /app

# Copiar primero los archivos de configuración y dependencias
COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copiar datos y archivos de configuración
COPY merge_user_altlantico_bolivar_no_duplicates.csv ./
COPY progreso_resenas_usuarios.json ./
COPY cookies_foursquare.json ./

# Copiar código fuente
COPY scraping_parallel.py ./

# Crear directorio para resultados
RUN mkdir -p /app/resultados/tips /app/resultados/users

# Verificar que los archivos existan
RUN ls -la /app/

CMD ["python", "scraping_parallel.py"]