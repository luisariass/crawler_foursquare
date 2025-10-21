#!/bin/bash

set -e

echo "[INFO] Iniciando despliegue en VM..."

PROJECT_ROOT="/home/scraper/proyecto_scrapping"

cd ${PROJECT_ROOT}

if [[ ! -f .env ]]; then
    echo "[ERROR] Archivo .env no encontrado"
    exit 1
fi

mkdir -p logs

echo "[INFO] Descargando imagen desde Docker Hub..."
docker pull luisarias/foursquare_sities_scraper:latest

echo "[INFO] Deteniendo contenedor anterior..."
docker-compose -f docker-compose.yml down 2>/dev/null || true

echo "[INFO] Limpiando volúmenes huérfanos..."
docker volume prune -f 2>/dev/null || true

echo "[INFO] Iniciando contenedor..."
docker-compose -f docker-compose.yml up -d

echo "[INFO] Esperando a que el contenedor este listo..."
sleep 10

echo "[INFO] Verificando estado del contenedor..."
docker-compose -f docker-compose.yml ps

echo "[INFO] Mostrando logs iniciales..."
docker-compose -f docker-compose.yml logs --tail=50

echo "[INFO] Despliegue completado exitosamente"