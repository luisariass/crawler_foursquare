#!/bin/bash
# filepath: c:\Users\luisarias\Documents\proyecto_scrapping\model_sities\script\deploy.sh

set -e

echo "[INFO] Iniciando despliegue en VM..."

if [ ! -f .env.production ]; then
    echo "[ERROR] Archivo .env.production no encontrado"
    exit 1
fi

mkdir -p logs

echo "[INFO] Construyendo imagen Docker..."
docker-compose -f docker-compose.yml build --no-cache

echo "[INFO] Deteniendo contenedor anterior..."
docker-compose -f docker-compose.yml down 2>/dev/null || true

echo "[INFO] Limpiando volúmenes huérfanos..."
docker volume prune -f 2>/dev/null || true

echo "[INFO] Iniciando contenedor..."
docker-compose -f docker-compose.yml up -d

echo "[INFO] Esperando a que el contenedor esté listo..."
sleep 10

echo "[INFO] Verificando estado del contenedor..."
docker-compose -f docker-compose.yml ps

echo "[INFO] Mostrando logs iniciales..."
docker-compose -f docker-compose.yml logs --tail=50

echo "[INFO] Despliegue completado exitosamente"