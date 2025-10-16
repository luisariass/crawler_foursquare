#!/bin/bash

CONTAINER_SCRAPER="foursquare_sities_prod"
PROJECT_ROOT="/home/scraper/proyecto_scrapping"

check_containers() {
    echo "[INFO] Estado del contenedor:"
    docker ps --filter "name=${CONTAINER_SCRAPER}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

check_scraper_health() {
    echo "[INFO] Verificando salud del scraper..."
    HEALTH=$(docker inspect --format='{{.State.Health.Status}}' ${CONTAINER_SCRAPER} 2>/dev/null || echo "no disponible")
    echo "[INFO] Estado de salud: ${HEALTH}"
}

show_logs() {
    LINES=${2:-50}
    echo "[INFO] Ultimas ${LINES} lineas de logs:"
    docker logs --tail=${LINES} ${CONTAINER_SCRAPER}
}

show_stats() {
    echo "[INFO] Estadisticas de recursos:"
    docker stats --no-stream ${CONTAINER_SCRAPER}
}

restart_container() {
    echo "[INFO] Reiniciando contenedor..."
    cd ${PROJECT_ROOT}
    docker-compose -f docker-compose.yml restart
    sleep 10
    check_containers
}

stop_container() {
    echo "[INFO] Deteniendo contenedor..."
    cd ${PROJECT_ROOT}
    docker-compose -f docker-compose.yml down
}

start_container() {
    echo "[INFO] Iniciando contenedor..."
    cd ${PROJECT_ROOT}
    docker-compose -f docker-compose.yml up -d
    sleep 10
    check_containers
}

case "${1}" in
    status)
        check_containers
        check_scraper_health
        ;;
    logs)
        show_logs "$@"
        ;;
    stats)
        show_stats
        ;;
    restart)
        restart_container
        ;;
    stop)
        stop_container
        ;;
    start)
        start_container
        ;;
    full)
        check_containers
        check_scraper_health
        show_logs
        ;;
    *)
        echo "Uso: $0 {status|logs|stats|restart|stop|start|full}"
        echo ""
        echo "Comandos:"
        echo "  status   - Muestra el estado del contenedor"
        echo "  logs     - Muestra los ultimos logs (logs [cantidad])"
        echo "  stats    - Muestra estadisticas de recursos"
        echo "  restart  - Reinicia el contenedor"
        echo "  stop     - Detiene el contenedor"
        echo "  start    - Inicia el contenedor"
        echo "  full     - Muestra informacion completa"
        exit 1
        ;;
esac