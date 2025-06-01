FROM python:3.13-slim

# Instala dependencias del sistema para Playwright
RUN apt-get update && apt-get install -y wget gnupg libnss3 libatk-bridge2.0-0 libgtk-3-0 libxss1 libasound2 libgbm1 libxshmfence1 libxcomposite1 libxdamage1 libxrandr2 libu2f-udev libvulkan1 fonts-liberation libappindicator3-1 xdg-utils

WORKDIR /app

# Copia solo los archivos necesarios
COPY scraping.py ./
COPY requirements.txt ./
COPY cookies_foursquare.json ./

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Instala navegadores de Playwright
RUN python -m playwright install --with-deps

CMD ["python", "scraping.py"]